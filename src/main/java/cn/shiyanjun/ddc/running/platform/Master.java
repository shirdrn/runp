package cn.shiyanjun.ddc.running.platform;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONObject;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Envelope;

import cn.shiyanjun.ddc.api.Context;
import cn.shiyanjun.ddc.api.LifecycleAware;
import cn.shiyanjun.ddc.api.common.AbstractComponent;
import cn.shiyanjun.ddc.api.common.ContextImpl;
import cn.shiyanjun.ddc.api.constants.JSONKeys;
import cn.shiyanjun.ddc.api.constants.TaskType;
import cn.shiyanjun.ddc.api.utils.NamedThreadFactory;
import cn.shiyanjun.ddc.network.NettyRpcServer;
import cn.shiyanjun.ddc.network.common.LocalMessage;
import cn.shiyanjun.ddc.network.common.MessageDispatcher;
import cn.shiyanjun.ddc.network.common.NettyRpcEndpoint;
import cn.shiyanjun.ddc.network.common.RpcMessage;
import cn.shiyanjun.ddc.network.common.RpcMessageHandler;
import cn.shiyanjun.ddc.network.common.RunnableMessageListener;
import cn.shiyanjun.ddc.running.platform.common.AbstractRunnableConsumer;
import cn.shiyanjun.ddc.running.platform.common.EndpointThread;
import cn.shiyanjun.ddc.running.platform.common.MQAccessService;
import cn.shiyanjun.ddc.running.platform.common.RabbitMQAccessService;
import cn.shiyanjun.ddc.running.platform.constants.MessageType;
import cn.shiyanjun.ddc.running.platform.constants.RunpConfigKeys;
import cn.shiyanjun.ddc.running.platform.master.MasterMessageDispatcher;
import cn.shiyanjun.ddc.running.platform.master.MasterRpcMessageHandler;
import cn.shiyanjun.ddc.running.platform.utils.ResourceUtils;

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
	private final String id = "Master";
	private NettyRpcEndpoint endpoint;
	private final RpcMessageHandler rpcMessageHandler;
	private final MessageDispatcher dispatcher;
	private final TaskAssignmentProcessor taskAssignment;
	private ExecutorService executorService;
	private final String rabbitmqConfig = "rabbitmq.properties";
	private final MQAccessService taskRequestMQAccessService;
	private final MQAccessService taskResultMQAccessService;
	private final AtomicLong idGenerator;
	private EndpointThread endpointThread;
	
	public Master(Context context) {
		super(context);
		dispatcher = new MasterMessageDispatcher(id, context);
		rpcMessageHandler = new MasterRpcMessageHandler(context, dispatcher);
		dispatcher.setRpcMessageHandler(rpcMessageHandler);
		taskAssignment = new TaskAssignmentProcessor(MessageType.TASK_ASSIGNMENT.getCode());
		
		ResourceUtils.registerResource(rabbitmqConfig, ConnectionFactory.class);
		final ConnectionFactory connectionFactory = ResourceUtils.getResource(ConnectionFactory.class);
		String taskRequestQName = context.get(RunpConfigKeys.MQ_TASK_REQUEST_QUEUE_NAME);
		String taskResultQName = context.get(RunpConfigKeys.MQ_TASK_RESULT_QUEUE_NAME);
		taskRequestMQAccessService = new RabbitMQAccessService(taskRequestQName, connectionFactory);
		taskResultMQAccessService = new RabbitMQAccessService(taskResultQName, connectionFactory);
		idGenerator = new AtomicLong(System.currentTimeMillis());
		endpoint = NettyRpcEndpoint.newEndpoint(NettyRpcServer.class, context, rpcMessageHandler);
		endpointThread = new EndpointThread();
	}
	
	@Override
	public void start() {
		try {
			LOG.info("Starting server endpoint...");
			endpointThread.setEndpoint(endpoint);
			new Thread(endpointThread).start();
			Thread.sleep(3000);
			LOG.info("Server started.");
			
			LOG.info("Starting master dispatcher...");
			dispatcher.register(taskAssignment);
			dispatcher.start();
			LOG.info("Master dispatcher started.");
			
			taskRequestMQAccessService.start();
			taskResultMQAccessService.start();
			
//			executorService = Executors.newFixedThreadPool(1, new NamedThreadFactory("MASTER"));
//			executorService.execute(new TaskRequestMQMessageConsumer(taskRequestMQAccessService.getQueueName(), taskRequestMQAccessService.getChannel()));
//			executorService.execute(new MockedMQProducer());
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void stop() {
		taskRequestMQAccessService.stop();
		taskResultMQAccessService.stop();
		endpoint.stop();		
	}
	
	final class TaskAssignmentProcessor extends RunnableMessageListener<LocalMessage> {

		public TaskAssignmentProcessor(int messageType) {
			super(messageType);
		}
		
		@Override
		public void handle(LocalMessage message) {
			dispatcher.getRpcMessageHandler().ask(message);
			
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
					RpcMessage taskMsg = new RpcMessage();
					taskMsg.setId(idGenerator.incrementAndGet());
					taskMsg.setType(MessageType.TASK_ASSIGNMENT.getCode());
					taskMsg.setBody(taskReq.toJSONString());
					LocalMessage m = new LocalMessage();
					m.setFromEndpointId(id);
					// TODO
					m.setToEndpointId(null);
					taskAssignment.addMessage(m);
					
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
					message.put(JSONKeys.TYPE, TaskType.GREEN_PLUM);
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
		final Context context = new ContextImpl();
		Master master = new Master(context);
		master.start();		
	}

}
