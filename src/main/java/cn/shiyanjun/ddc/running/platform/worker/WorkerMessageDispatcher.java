package cn.shiyanjun.ddc.running.platform.worker;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Queues;

import cn.shiyanjun.ddc.api.utils.NamedThreadFactory;
import cn.shiyanjun.ddc.network.common.AbstractMessageDispatcher;
import cn.shiyanjun.ddc.network.common.PeerMessage;
import cn.shiyanjun.ddc.network.common.RpcMessage;
import cn.shiyanjun.ddc.network.common.RunnableMessageListener;
import cn.shiyanjun.ddc.running.platform.common.WorkerContext;
import cn.shiyanjun.ddc.running.platform.constants.JsonKeys;
import cn.shiyanjun.ddc.running.platform.constants.MessageType;
import cn.shiyanjun.ddc.running.platform.constants.RunpConfigKeys;
import cn.shiyanjun.ddc.running.platform.constants.Status;
import cn.shiyanjun.ddc.running.platform.utils.Time;
import cn.shiyanjun.ddc.running.platform.utils.Utils;

public class WorkerMessageDispatcher extends AbstractMessageDispatcher {

	private static final Log LOG = LogFactory.getLog(WorkerMessageDispatcher.class);
	private final HeartbeatReporter heartbeatReporter;
	private final TaskProgressReporter taskProgressReporter;
	private final RemoteMessageReceiver remoteMessageReceiver;
	private final WorkerContext workerContext;
	private final String workerId;
	private volatile String masterId;
	private final String workerHost;
	private final int heartbeatIntervalMillis;
	private final BlockingQueue<PeerMessage> waitingTasksQueue = Queues.newLinkedBlockingQueue();
	private ScheduledExecutorService scheduledExecutorService;
	private ClientConnectionManager clientConnectionManager;
	
	public WorkerMessageDispatcher(WorkerContext workerContext) {
		super(workerContext.getContext());
		this.workerContext = workerContext;
		masterId = workerContext.getMasterId();
		workerId = workerContext.getThisPeerId();
		workerHost = workerContext.getContext().get(RunpConfigKeys.WORKER_HOST);
		heartbeatIntervalMillis = workerContext.getContext().getInt(RunpConfigKeys.WORKER_HEARTBEAT_INTERVALMILLIS, 60000);
		
		// create & register message listener
		heartbeatReporter = new HeartbeatReporter(MessageType.WORKER_REGISTRATION, MessageType.HEART_BEAT);
		taskProgressReporter = new TaskProgressReporter(MessageType.TASK_PROGRESS);
		remoteMessageReceiver = new RemoteMessageReceiver(MessageType.TASK_ASSIGNMENT, MessageType.ACK_WORKER_REGISTRATION);
		register(taskProgressReporter);
		register(heartbeatReporter);
		register(remoteMessageReceiver);
		
	}

	@Override
	public void start() {
		super.start();
		clientConnectionManager = workerContext.getClientConnectionManager();
		
		// try to register to master
		clientConnectionManager.registerToMaster();
		
		// send heartbeat to master periodically
		scheduledExecutorService = Executors.newScheduledThreadPool(1, new NamedThreadFactory("HEARTBEAT"));
		scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
			
			@Override
			public void run() {
				PeerMessage message = prepareHeartbeatMessage();
				heartbeatReporter.addMessage(message);
				LOG.debug("Heartbeart prepared: id=" + message.getRpcMessage().getId() + ", body=" + message.getRpcMessage().getBody());
			}
			
			private PeerMessage prepareHeartbeatMessage() {
				RpcMessage hb = new RpcMessage(workerContext.getMessageidGenerator().incrementAndGet(), MessageType.HEART_BEAT.getCode());
				hb.setNeedReply(false);
				JSONObject body = new JSONObject();
				body.put(JsonKeys.WORKER_ID, workerId);
				body.put(JsonKeys.WORKER_HOST, workerHost);
				hb.setBody(body.toJSONString());
				hb.setTimestamp(Time.now());
				
				PeerMessage message = new PeerMessage();
				message.setRpcMessage(hb);
				message.setFromEndpointId(workerId);
				message.setToEndpointId(masterId);
				message.setChannel(workerContext.getChannel(masterId));
				return message;
			}
			
		}, 3000, heartbeatIntervalMillis, TimeUnit.MILLISECONDS);
	}
	
	final class RemoteMessageReceiver extends RunnableMessageListener<PeerMessage> {
		
		
		public RemoteMessageReceiver(MessageType... messageTypes) {
			super(Utils.toIntegerArray(messageTypes));
		}

		@Override
		public void handle(PeerMessage message) {
			RpcMessage rpcMessage = message.getRpcMessage();
			Optional<MessageType> messageType = MessageType.fromCode(rpcMessage.getType());
			messageType.ifPresent(mt -> {
				switch(mt) {
					case TASK_ASSIGNMENT:
						waitingTasksQueue.add(message);
						LOG.info("Assigned task received: message=" + rpcMessage);
						break;
					case ACK_WORKER_REGISTRATION:
						JSONObject repliedAck = JSONObject.parseObject(message.getRpcMessage().getBody());
						String status = repliedAck.getString(JsonKeys.STATUS);
						if(Status.SUCCEES.toString().equals(status)) {
							masterId = repliedAck.getString(JsonKeys.MASTER_ID);
							clientConnectionManager.notifyRegistered();
							LOG.debug("Succeeded to notify dispatcher.");
							LOG.info("Worker registered: id=" + rpcMessage.getId() + ", type=" + rpcMessage.getType() + ", body=" + rpcMessage.getBody());
						} else {
							LOG.info("Worker registration failed: id=" + rpcMessage.getId() + ", type=" + rpcMessage.getType() + ", body=" + rpcMessage.getBody());
							clientConnectionManager.notifyRegistered();
						}
						break;
					default:
				}
			});
		}

	}
	
	/**
	 * Send task progress report messages to remote <code>Master</code>.
	 * 
	 * @author yanjun
	 */
	final class TaskProgressReporter extends RunnableMessageListener<PeerMessage> {
		
		public TaskProgressReporter(MessageType messageType) {
			super(messageType.getCode());
		}

		@Override
		public void handle(PeerMessage message) {
			getRpcService().send(message);
		}

	}
	
	/**
	 * Send heartbeat messages to remote <code>Master</code>.
	 * 
	 * @author yanjun
	 */
	final class HeartbeatReporter extends RunnableMessageListener<PeerMessage> {
		
		public HeartbeatReporter(MessageType... messageTypes) {
			super(Utils.toIntegerArray(messageTypes));
		}
		
		
		@Override
		public void start() {
			super.start();
		}
		
		@Override
		public void stop() {
			super.stop();
		}

		@Override
		public void handle(PeerMessage message) {
			RpcMessage m = message.getRpcMessage();
			try {
				Optional<MessageType> messageType = MessageType.fromCode(m.getType());
				if(messageType.isPresent()) {
					switch(messageType.get()) {
						case WORKER_REGISTRATION:
							getRpcService().ask(message);
							LOG.info("Registration message sent: message=" + m);
							break;
						case HEART_BEAT:
							getRpcService().send(message);
							LOG.debug("Heartbeart sent: message=" + m);
							break;
						default:
							LOG.warn("Unknown received message: messageType=" + messageType);
					}
				}
			} catch (Exception e) {
				LOG.warn("Fail to handle received message: rpcMessage=" + m, e);
			}
		}

	}
	
}
