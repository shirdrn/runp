package cn.shiyanjun.ddc.running.platform.master;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.shiyanjun.ddc.api.Context;
import cn.shiyanjun.ddc.api.network.MessageListener;
import cn.shiyanjun.ddc.api.utils.NamedThreadFactory;
import cn.shiyanjun.ddc.network.common.AbstractMessageListener;
import cn.shiyanjun.ddc.network.common.RpcChannelHandler;
import cn.shiyanjun.ddc.network.common.RpcMessage;
import cn.shiyanjun.ddc.running.platform.common.TaskAssignmentProtocol;
import cn.shiyanjun.ddc.running.platform.common.WorkerInfo;
import cn.shiyanjun.ddc.running.platform.constants.JsonKeys;
import cn.shiyanjun.ddc.running.platform.constants.MessageTypes;
import net.sf.json.JSONObject;

/**
 * In <code>Master</code> side, its responsibility is to handle messages 
 * from <code>Worker</code> side, such as consuming received heartbeat, 
 * and in current side, such as sending messages to <code>Worker</code>.
 * 
 * @author yanjun
 */
public class MasterMessageListener extends AbstractMessageListener implements MessageListener<RpcMessage>, TaskAssignmentProtocol {

	private static final Log LOG = LogFactory.getLog(MasterMessageListener.class);
	private final ExecutorService executorService;
	private final ConcurrentMap<Long, RpcMessage> taskAssignmentAckPendingQueue = new ConcurrentHashMap<>();
	private final ConcurrentMap<String, WorkerInfo> workers = new ConcurrentHashMap<>();
	
	public MasterMessageListener(Context context, RpcChannelHandler rpcChannelHandler) {
		super(context, rpcChannelHandler);
		executorService = Executors.newFixedThreadPool(3, new NamedThreadFactory("MSG-LISTENER"));
		registerAndStart(MessageTypes.HEART_BEAT, new HearbeatProcessor(MessageTypes.HEART_BEAT));
		registerAndStart(MessageTypes.TASK_ASSIGNMENT, new TaskAssignmentProcessor(MessageTypes.TASK_ASSIGNMENT));
		registerAndStart(MessageTypes.ACK_TASK_ASSIGNMENT, new AckTaskAssignmentProcessor(MessageTypes.ACK_TASK_ASSIGNMENT));
		
		addShutdownHook(new CheckingMessageQueuesBeforeJvmExit());
	}
	
	private void registerAndStart(int messageType, MessageProcessor processor) {
		registerQueue(messageType);
		executorService.execute(processor);
	}
	
	final class CheckingMessageQueuesBeforeJvmExit extends Thread {
		
		@Override
		public void run() {
			boolean isNotAllQueueEmpty = true;
			while(isNotAllQueueEmpty) {
				try {
					boolean isEmpty = true;
					for(BlockingQueue<RpcMessage> q : registeredMessageQueues()) {
						if(!q.isEmpty()) {
							isEmpty = false;
							break;
						}
					}
					if(isEmpty) {
						isNotAllQueueEmpty = false;
					} else {
						Thread.sleep(1000);
					}
				} catch (InterruptedException e) { }
			}
			executorService.shutdown();
		}
	}

	@Override
	public void handle(RpcMessage message) {
		if(message != null) {
			final BlockingQueue<RpcMessage> q = super.selectMessageQueue(message.getType());
			if(q != null) {
				q.add(message);
			} else {
				LOG.warn("Unknown RPC message: " + message);
			}
		}		
	}

	private final class HearbeatProcessor extends MessageProcessor {
		
		public HearbeatProcessor(int messageType) {
			super(messageType);
		}

		@Override
		protected void processMessage(RpcMessage msg) throws Exception {
			long id = msg.getId();
			String body = msg.getBody();
			if(body != null) {
				LOG.info("Heartbeat message: id=" + id + ", body=" + body);
				JSONObject jsonBody = JSONObject.fromObject(body);
				String host = jsonBody.getString(JsonKeys.HOST);
				// record worker information
				WorkerInfo workerInfo = new WorkerInfo();
				WorkerInfo oldWorkerInfo = workers.putIfAbsent(host, workerInfo);
				if(oldWorkerInfo == null) {
					
				}
			}
		}
	}
	
	private final class TaskAssignmentProcessor extends MessageProcessor {
		
		public TaskAssignmentProcessor(int messageType) {
			super(messageType);
		}
		
		@Override
		protected void processMessage(RpcMessage msg) throws Exception {
			getRpcChannelHandler().ask(msg);
			taskAssignmentAckPendingQueue.putIfAbsent(msg.getId(), msg);
			LOG.info("Assign task: id=" + msg.getId() + ", body=" + msg.getBody());
		}
	}

	@Override
	public void assign(RpcMessage msg) {
		super.selectMessageQueue(msg.getType()).add(msg);	
	}
	
	private final class AckTaskAssignmentProcessor extends MessageProcessor {

		public AckTaskAssignmentProcessor(int type) {
			super(type);
		}

		@Override
		protected void processMessage(RpcMessage msg) throws Exception {
			final RpcMessage ackPendingMsg = taskAssignmentAckPendingQueue.get(msg.getId());
			if(ackPendingMsg != null) {
				taskAssignmentAckPendingQueue.remove(msg.getId(), ackPendingMsg);
			}
			LOG.info("Acked assigned task: id=" + msg.getId());
		}
		
	}
	
}
