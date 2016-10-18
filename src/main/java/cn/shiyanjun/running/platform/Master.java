package cn.shiyanjun.running.platform;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Throwables;
import com.google.common.collect.Queues;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Envelope;

import cn.shiyanjun.platform.api.LifecycleAware;
import cn.shiyanjun.platform.api.common.ContextImpl;
import cn.shiyanjun.platform.api.constants.TaskStatus;
import cn.shiyanjun.platform.api.constants.TaskType;
import cn.shiyanjun.platform.api.utils.NamedThreadFactory;
import cn.shiyanjun.platform.network.NettyRpcServer;
import cn.shiyanjun.platform.network.api.MessageDispatcher;
import cn.shiyanjun.platform.network.common.PeerMessage;
import cn.shiyanjun.platform.network.common.RpcMessage;
import cn.shiyanjun.platform.network.common.RpcService;
import cn.shiyanjun.platform.network.common.RunnableMessageListener;
import cn.shiyanjun.running.platform.api.ConnectionManager;
import cn.shiyanjun.running.platform.api.MQAccessService;
import cn.shiyanjun.running.platform.api.TaskScheduler;
import cn.shiyanjun.running.platform.common.AbstractRunnableConsumer;
import cn.shiyanjun.running.platform.component.RabbitMQAccessService;
import cn.shiyanjun.running.platform.component.TaskSchedulerImpl;
import cn.shiyanjun.running.platform.constants.JsonKeys;
import cn.shiyanjun.running.platform.constants.MessageType;
import cn.shiyanjun.running.platform.constants.RunpConfigKeys;
import cn.shiyanjun.running.platform.master.MasterChannelHandler;
import cn.shiyanjun.running.platform.master.MasterContext;
import cn.shiyanjun.running.platform.master.MasterMessageDispatcher;
import cn.shiyanjun.running.platform.master.MasterRpcService;
import cn.shiyanjun.running.platform.master.ServerConnectionManager;
import cn.shiyanjun.running.platform.master.WorkOrder;
import cn.shiyanjun.running.platform.utils.ResourceUtils;
import cn.shiyanjun.running.platform.utils.Time;

/**
 * Master is the coordinator of running platform, its responsibility is
 * to accept {@link Worker}s' heartbeat messages to acquire the states and resources
 *  of each worker node. And finally the Scheduling Platform should be told to
 *  decide next scheduling choice.
 * 
 * @author yanjun
 */
public final class Master implements LifecycleAware {

	private static final Log LOG = LogFactory.getLog(Master.class);
	protected final MasterContext masterContext;
	private final ConnectionManager connectionManager;
	private final MessageDispatcher dispatcher;
	private final TaskAssignmentProcessor taskAssignmentProcessor;
	private ExecutorService executorService;
	private final String rabbitmqConfig = "rabbitmq.properties";
	private final MQAccessService taskRequestMQAccessService;
	private final MQAccessService taskResultMQAccessService;
	private final AtomicLong messageIdGenerator;
	private final RpcService rpcService;
	private volatile boolean running = true;
	private final TaskScheduler taskScheduler;
	private final BlockingDeque<WaitingTask> waitingTasks = Queues.newLinkedBlockingDeque();
	private final BlockingQueue<PendingAssignedTask> pendingAssignedTasks = Queues.newLinkedBlockingQueue();
	private final BlockingQueue<RunningTask> runningTasks = Queues.newLinkedBlockingDeque();
	private final BlockingQueue<CompletedTask> completedTasks = Queues.newLinkedBlockingDeque();
	
	public Master(MasterContext masterContext) {
		this.masterContext = masterContext;
		
		dispatcher = new MasterMessageDispatcher(masterContext);
		connectionManager = new ServerConnectionManager(masterContext);
		masterContext.setMessageDispatcher(dispatcher);
		
		rpcService = new MasterRpcService(masterContext);
		masterContext.setRpcService(rpcService);
		
		taskScheduler = new TaskSchedulerImpl(masterContext);
		masterContext.setTaskScheduler(taskScheduler);
		
		taskAssignmentProcessor = new TaskAssignmentProcessor(MessageType.TASK_ASSIGNMENT);
		
		// create Rabbit MQ access service
		ResourceUtils.registerResource(rabbitmqConfig, ConnectionFactory.class);
		final ConnectionFactory connectionFactory = ResourceUtils.getResource(ConnectionFactory.class);
		String taskRequestQName = masterContext.getContext().get(RunpConfigKeys.MQ_TASK_REQUEST_QUEUE_NAME);
		String taskResultQName = masterContext.getContext().get(RunpConfigKeys.MQ_TASK_RESULT_QUEUE_NAME);
		taskRequestMQAccessService = new RabbitMQAccessService(taskRequestQName, connectionFactory);
		taskResultMQAccessService = new RabbitMQAccessService(taskResultQName, connectionFactory);
		
		messageIdGenerator = new AtomicLong(Time.now());
		dispatcher.register(new TaskProgressReceiver(MessageType.TASK_PROGRESS));
	}

	@Override
	public void start() {
		try {
			rpcService.start();
			executorService = Executors.newCachedThreadPool(new NamedThreadFactory("MASTER"));
			
			// start RPC endpoint
			connectionManager.startEndpoint(NettyRpcServer.class, MasterChannelHandler.class);
			
			LOG.info("Starting master dispatcher...");
			dispatcher.register(taskAssignmentProcessor);
			dispatcher.start();
			LOG.info("Master dispatcher started.");
			
			taskRequestMQAccessService.start();
			taskResultMQAccessService.start();
	
			executorService.execute(new TaskRequestMQMessageConsumer(taskRequestMQAccessService.getQueueName(), taskRequestMQAccessService.getChannel()));
			executorService.execute(new SchedulingThread());
			executorService.execute(new MockedMQProducer());
			connectionManager.getEndpoint().await();
		} catch (Exception e) {
			Throwables.propagate(e);
		}
	}
	
	@Override
	public void stop() {
		taskRequestMQAccessService.stop();
		taskResultMQAccessService.stop();
		connectionManager.getEndpoint().stop();
		executorService.shutdown();
		running = false;
	}
	
	final class TaskAssignmentProcessor extends RunnableMessageListener<PeerMessage> {

		public TaskAssignmentProcessor(MessageType messageType) {
			super(messageType.getCode());
		}
		
		@Override
		public void handle(PeerMessage message) {
			rpcService.ask(message);
			LOG.info("Task assigning: targerWorker=" + message.getToEndpointId() + ", rpcMessage=" + message.getRpcMessage());
		}

	}
	
	private final class SchedulingThread implements Runnable {
		
		@Override
		public void run() {
			while(running) {
				try {
					// take from waitingTasks queue
					WaitingTask task = waitingTasks.takeFirst();
					Optional<WorkOrder> workOrder = taskScheduler.resourceOffer(task.taskType);
					if(workOrder.isPresent()) {
						PendingAssignedTask assigningTask = new PendingAssignedTask(task, workOrder.get().getTargetWorkerId());
						
						RpcMessage rpcMessage = new RpcMessage();
						rpcMessage.setId(assigningTask.id);
						rpcMessage.setBody(assigningTask.taskData.toString());
						rpcMessage.setType(assigningTask.taskType.getCode());
						rpcMessage.setTimestamp(Time.now());
						
						PeerMessage peerMessage = new PeerMessage();
						peerMessage.setRpcMessage(rpcMessage);
						peerMessage.setFromEndpointId(masterContext.getPeerId());
						peerMessage.setToEndpointId(assigningTask.workerId);
						
						taskAssignmentProcessor.addMessage(peerMessage);
						pendingAssignedTasks.add(assigningTask);
						LOG.info("Task scheduled: " + rpcMessage);
					} else {
						waitingTasks.putLast(task);
						LOG.debug("Resource insufficient, push back to waiting queue: taskType=" + task.taskType + ", task=" + task);
						Thread.sleep(1000);
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
		}
	}
	
	/**
	 * Consume scheduled task messages from RabbitMQ, and then deliver transformed 
	 * {@link RpcMessage} messages to the <code>Worker</code>.
	 * 
	 * @author yanjun
	 */
	private final class TaskRequestMQMessageConsumer extends AbstractRunnableConsumer {
		
		public TaskRequestMQMessageConsumer(String queueName, Channel channel) {
			super(queueName, channel);
		}
		
		@Override
		public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body) throws IOException {
			if(body != null) {
				try {
					// resolve the returned message
					long deliveryTag = envelope.getDeliveryTag();
					String message = new String(body, "UTF-8");
					LOG.info("Task received: consumerTag=" + consumerTag + ", deliveryTag=" + deliveryTag + ", message=" + message);
					
					JSONObject taskReq = JSONObject.parseObject(message);
					if(!taskReq.isEmpty()) {
						long id = messageIdGenerator.incrementAndGet();
						WaitingTask task = new WaitingTask(id, taskReq, getChannel(), deliveryTag);
						waitingTasks.putLast(task);
						LOG.info("Add task to waiting queue: " + task);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	private final class MockedMQProducer extends Thread {
		
		@Override
		public void run() {
			while(true) {
				try {
					JSONObject body = new JSONObject(true);
					body.put(JsonKeys.TASK_TYPE, TaskType.GREEN_PLUM.getCode());
					body.put(JsonKeys.TASK_TYPE_DESC, TaskType.GREEN_PLUM);
					JSONObject params = new JSONObject(true);
					params.put("a", "aaaaaaaaaa");
					params.put("b", "bbbbbbbbbb");
					params.put("c", "cccccccccc");
					body.put(JsonKeys.TASK_PARAMS, params);
					taskRequestMQAccessService.produceMessage(body.toJSONString());
					LOG.info("Message published: " + body);
					Thread.sleep(60000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
		}
	}
	
	/**
	 * Receive and handle task progress messages reported by <code>Worker</code>.
	 * 
	 * @author yanjun
	 */
	final class TaskProgressReceiver extends RunnableMessageListener<PeerMessage> {
		
		public TaskProgressReceiver(MessageType messageType) {
			super(messageType.getCode());
		}

		@Override
		public void handle(PeerMessage message) {
			LOG.debug("Task progress received: fromEndpointId=" + message.getFromEndpointId() + ", channel=" + message.getChannel() + ", " + message.getRpcMessage());
			RpcMessage rpcMessage = message.getRpcMessage();
			JSONObject body = JSONObject.parseObject(rpcMessage.getBody());
			long taskId = body.getLongValue(JsonKeys.TASK_ID);
			String status = body.getString(JsonKeys.TASK_STATUS);
			TaskStatus taskStatus = TaskStatus.valueOf(status);
			Optional<RunningTask> rTask = getRunningTask(taskId);
			switch(taskStatus) {
				case RUNNING:
					if(rTask.isPresent()) {
						rTask.get().latUpdateTs = Time.now();
					} else {
						Optional<PendingAssignedTask> pTask = getPendingAssignedTask(taskId);
						pTask.ifPresent(task -> {
							try {
								pTask.get().channel.basicAck(pTask.get().deliveryTag, false);
								pendingAssignedTasks.remove(pTask.get());
								RunningTask runningTask = new RunningTask(pTask.get());
								runningTask.latUpdateTs = Time.now();
								runningTasks.add(runningTask);
								LOG.info("Task assigned: workerId=" + runningTask.workerId + ", " + rpcMessage);
							} catch (IOException e) {
								e.printStackTrace();
							}
						});
					}
					break;
					
				case SUCCEEDED:
				case FAILED:
					rTask.ifPresent(t -> {
						try {
							// move task from queue: runningTasks => completedTasks
							CompletedTask cTask = new CompletedTask(t, taskStatus);
							completedTasks.put(cTask);
							runningTasks.remove(t);
							
							// release resource
							masterContext.releaseResource(t.workerId, t.taskType);
							LOG.info("Task completed: taskId=" + taskId + ", taskType=" + cTask.taskType + ", workerId=" + t.workerId + ", taskStatus=" + taskStatus);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					});
					break;
				default:
			}
		}

	}
	
	class WaitingTask {
		
		final long id;
		final TaskType taskType;
		final JSONObject taskData;
		final Channel channel;
		final long deliveryTag;
		long lastUpdateTs;
		
		public WaitingTask(long id, JSONObject taskData, Channel channel, long deliveryTag) {
			this.id = id;
			this.taskType = TaskType.fromCode(taskData.getIntValue(JsonKeys.TASK_TYPE)).get();
			this.taskData = taskData;
			this.channel = channel;
			this.deliveryTag = deliveryTag;
			this.lastUpdateTs = Time.now();
		}
		
		@Override
		public int hashCode() {
			return String.valueOf(id).hashCode();
		}
		
		@Override
		public boolean equals(Object obj) {
			WaitingTask other = (WaitingTask) obj;
			return this.id == other.id;
		}
		
		@Override
		public String toString() {
			return new StringBuffer()
					.append("id=").append(id)
					.append(", taskType=").append(taskType)
					.append(", delieryTag=").append(deliveryTag)
					.append(", taskData=").append(taskData)
					.append(", channel=").append(channel)
					.append(", lastUpdateTs=").append(lastUpdateTs)
					.toString();
		}
	}
	
	class PendingAssignedTask extends WaitingTask {
		
		String workerId;
		
		public PendingAssignedTask(WaitingTask waitingTask, String workerId) {
			super(waitingTask.id, waitingTask.taskData, waitingTask.channel, waitingTask.deliveryTag);
			this.workerId = workerId;
			this.lastUpdateTs = Time.now();
		}
		
		@Override
		public int hashCode() {
			return super.hashCode();
		}
		
		@Override
		public boolean equals(Object obj) {
			return super.equals(obj);
		}
		
	}
	
	class RunningTask {
		
		final long id;
		String workerId;
		TaskType taskType;
		JSONObject taskData;
		long latUpdateTs;
		
		public RunningTask(PendingAssignedTask task) {
			super();
			this.id = task.id;
			this.workerId = task.workerId;
			this.taskType = task.taskType;
			this.taskData = task.taskData;
			this.latUpdateTs = Time.now();
		}
		
		public RunningTask(long id) {
			super();
			this.id = id;
		}
	
		public RunningTask(long id, TaskType taskType, JSONObject taskData) {
			super();
			this.id = id;
			this.taskType = taskType;
			this.taskData = taskData;
			this.latUpdateTs = Time.now();
		}
		
		@Override
		public int hashCode() {
			return String.valueOf(id).hashCode();
		}
		
		@Override
		public boolean equals(Object obj) {
			RunningTask other = (RunningTask) obj;
			return this.id == other.id;
		}
		
	}
	
	class CompletedTask extends RunningTask {
		
		final TaskStatus taskStatus;
		
		public CompletedTask(RunningTask task, TaskStatus taskStatus) {
			super(task.id, task.taskType, task.taskData);
			this.workerId = task.workerId;
			this.taskStatus = taskStatus;
		}
		
	}
	
	Optional<RunningTask> getRunningTask(long id) {
		return runningTasks.stream()
				.filter(r -> r.id == id)
				.findFirst();
	}
	
	Optional<PendingAssignedTask> getPendingAssignedTask(long id) {
		return pendingAssignedTasks.stream()
				.filter(p -> p.id == id)
				.findFirst();
	}
	
	public static void main(String[] args) {
		final MasterContext masterContext = new MasterContext(new ContextImpl());
		Master master = new Master(masterContext);
		master.start();		
	}

}
