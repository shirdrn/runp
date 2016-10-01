package cn.shiyanjun.ddc.running.platform;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Envelope;

import cn.shiyanjun.ddc.api.LifecycleAware;
import cn.shiyanjun.ddc.api.common.AbstractComponent;
import cn.shiyanjun.ddc.api.common.ContextImpl;
import cn.shiyanjun.ddc.api.constants.JSONKeys;
import cn.shiyanjun.ddc.api.constants.TaskType;
import cn.shiyanjun.ddc.api.utils.NamedThreadFactory;
import cn.shiyanjun.ddc.api.utils.Pair;
import cn.shiyanjun.ddc.network.NettyRpcServer;
import cn.shiyanjun.ddc.network.common.PeerMessage;
import cn.shiyanjun.ddc.network.common.MessageDispatcher;
import cn.shiyanjun.ddc.network.common.NettyRpcEndpoint;
import cn.shiyanjun.ddc.network.common.RpcMessage;
import cn.shiyanjun.ddc.network.common.RpcService;
import cn.shiyanjun.ddc.network.common.RunnableMessageListener;
import cn.shiyanjun.ddc.running.platform.api.MQAccessService;
import cn.shiyanjun.ddc.running.platform.common.AbstractRunnableConsumer;
import cn.shiyanjun.ddc.running.platform.common.RunpContext;
import cn.shiyanjun.ddc.running.platform.component.RabbitMQAccessService;
import cn.shiyanjun.ddc.running.platform.constants.MessageType;
import cn.shiyanjun.ddc.running.platform.constants.RunpConfigKeys;
import cn.shiyanjun.ddc.running.platform.master.MasterChannelHandler;
import cn.shiyanjun.ddc.running.platform.master.MasterMessageDispatcher;
import cn.shiyanjun.ddc.running.platform.master.MasterRpcService;
import cn.shiyanjun.ddc.running.platform.utils.ResourceUtils;
import cn.shiyanjun.ddc.running.platform.utils.Time;
import io.netty.channel.ChannelHandler;

/**
 * Master is the coordinator of running platform, its responsibility is
 * to accept {@link Worker}s' heartbeat messages to acquire the states and resources
 *  of each worker node. And finally the Scheduling Platform should be told to
 *  decide next scheduling choice.
 * 
 * @author yanjun
 */
public class Master extends AbstractComponent implements LifecycleAware {

	private static final Log LOG = LogFactory.getLog(Master.class);
	private final String id;
	private NettyRpcEndpoint endpoint;
	private final MessageDispatcher dispatcher;
	private final TaskAssignmentProcessor taskAssignment;
	private ExecutorService executorService;
	private final String rabbitmqConfig = "rabbitmq.properties";
	private final MQAccessService taskRequestMQAccessService;
	private final MQAccessService taskResultMQAccessService;
	private final AtomicLong idGenerator;
	private final RpcService rpcService;
	
	public Master(RunpContext context) {
		super(context.getContext());
		id = context.getContext().get(RunpConfigKeys.MASTER_ID, context.getMasterId());
		context.setMasterId(id);
		context.setThisPeerId(id);
		
		dispatcher = new MasterMessageDispatcher(context);
		context.setMessageDispatcher(dispatcher);
		rpcService = new MasterRpcService(context);
		context.setRpcService(rpcService);
		dispatcher.setRpcService(rpcService);
		
		List<Pair<Class<? extends ChannelHandler>, Object[]>> handlerInfos = Lists.newArrayList();
		handlerInfos.add(new Pair<Class<? extends ChannelHandler>, Object[]>(MasterChannelHandler.class, new Object[] {context.getContext(), dispatcher}));
		endpoint = NettyRpcEndpoint.newEndpoint(
				context.getContext(), 
				NettyRpcServer.class, 
				handlerInfos);
		
		taskAssignment = new TaskAssignmentProcessor(MessageType.TASK_ASSIGNMENT.getCode());
		
		ResourceUtils.registerResource(rabbitmqConfig, ConnectionFactory.class);
		final ConnectionFactory connectionFactory = ResourceUtils.getResource(ConnectionFactory.class);
		String taskRequestQName = context.getContext().get(RunpConfigKeys.MQ_TASK_REQUEST_QUEUE_NAME);
		String taskResultQName = context.getContext().get(RunpConfigKeys.MQ_TASK_RESULT_QUEUE_NAME);
		taskRequestMQAccessService = new RabbitMQAccessService(taskRequestQName, connectionFactory);
		taskResultMQAccessService = new RabbitMQAccessService(taskResultQName, connectionFactory);
		idGenerator = new AtomicLong(Time.now());
	}
	
	@Override
	public void start() {
		try {
			rpcService.start();
			executorService = Executors.newCachedThreadPool(new NamedThreadFactory("MASTER"));
			executorService.execute(new Runnable() {

				@Override
				public void run() {
					LOG.info("Starting server endpoint...");
					endpoint.start();					
					LOG.info("Server started.");
				}
				
			});
			Thread.sleep(1000);
			
			LOG.info("Starting master dispatcher...");
			dispatcher.register(taskAssignment);
			dispatcher.start();
			LOG.info("Master dispatcher started.");
			
			taskRequestMQAccessService.start();
			taskResultMQAccessService.start();
	
			executorService.execute(new TaskRequestMQMessageConsumer(taskRequestMQAccessService.getQueueName(), taskRequestMQAccessService.getChannel()));
//			executorService.execute(new MockedMQProducer());
		} catch (Exception e) {
			Throwables.propagate(e);
		}
	}

	@Override
	public void stop() {
		taskRequestMQAccessService.stop();
		taskResultMQAccessService.stop();
		endpoint.stop();
		executorService.shutdown();
	}
	
	final class TaskAssignmentProcessor extends RunnableMessageListener<PeerMessage> {

		public TaskAssignmentProcessor(int messageType) {
			super(messageType);
		}
		
		@Override
		public void handle(PeerMessage message) {
			dispatcher.getRpcService().ask(message);
			
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
		public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
				throws IOException {
			if(body != null) {
				// resolve the returned message
				long deliveryTag = envelope.getDeliveryTag();
				String message = new String(body, "UTF-8");
				LOG.info("Task request received: deliveryTag=" + deliveryTag + ", message=" + message);
				
				JSONObject taskReq = JSONObject.parseObject(message);
				if(!taskReq.isEmpty()) {
					RpcMessage rpcMessage = new RpcMessage();
					rpcMessage.setId(idGenerator.incrementAndGet());
					rpcMessage.setType(MessageType.TASK_ASSIGNMENT.getCode());
					rpcMessage.setBody(taskReq.toJSONString());
					
					PeerMessage peerMessage = new PeerMessage();
					peerMessage.setRpcMessage(rpcMessage);
					peerMessage.setFromEndpointId(id);
					// TODO
					peerMessage.setToEndpointId(null);
					taskAssignment.addMessage(peerMessage);
					
					getChannel().basicAck(deliveryTag, false);
				}
			}
		}
	}
	
	private final class MockedMQProducer extends Thread {
		
		@Override
		public void run() {
			while(true) {
				try {
					JSONObject message = new JSONObject(true);
					message.put(JSONKeys.TYPE, TaskType.GREEN_PLUM.getCode());
					taskRequestMQAccessService.produceMessage(message.toJSONString());
					LOG.info("Message published: " + message);
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
		}
	}
	
	public static void main(String[] args) {
		final RunpContext context = new RunpContext();
		context.setContext(new ContextImpl());
		Master master = new Master(context);
		master.start();		
	}

}
