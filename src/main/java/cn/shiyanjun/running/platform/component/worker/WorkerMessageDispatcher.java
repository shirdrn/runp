package cn.shiyanjun.running.platform.component.worker;

import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONObject;

import cn.shiyanjun.platform.api.utils.NamedThreadFactory;
import cn.shiyanjun.platform.network.common.AbstractMessageDispatcher;
import cn.shiyanjun.platform.network.common.PeerMessage;
import cn.shiyanjun.platform.network.common.RpcMessage;
import cn.shiyanjun.platform.network.common.RunnableMessageListener;
import cn.shiyanjun.running.platform.api.TaskLauncher;
import cn.shiyanjun.running.platform.constants.JsonKeys;
import cn.shiyanjun.running.platform.constants.MessageType;
import cn.shiyanjun.running.platform.constants.RunpConfigKeys;
import cn.shiyanjun.running.platform.constants.Status;
import cn.shiyanjun.running.platform.utils.Time;
import cn.shiyanjun.running.platform.utils.Utils;

public class WorkerMessageDispatcher extends AbstractMessageDispatcher {

	private static final Log LOG = LogFactory.getLog(WorkerMessageDispatcher.class);
	private final HeartbeatReporter heartbeatReporter;
	private final RunnableMessageListener<PeerMessage> taskProgressReporter;
	private final RemoteMessageReceiver remoteMessageReceiver;
	private final WorkerContext workerContext;
	private final String workerId;
	private volatile String masterId;
	private final String workerHost;
	private final int heartbeatIntervalMillis;
	private ScheduledExecutorService scheduledExecutorService;
	private ClientConnectionManager clientConnectionManager;
	
	public WorkerMessageDispatcher(WorkerContext workerContext) {
		super(workerContext.getContext());
		this.workerContext = workerContext;
		masterId = workerContext.getMasterId();
		workerId = workerContext.getPeerId();
		workerHost = workerContext.getContext().get(RunpConfigKeys.WORKER_HOST);
		heartbeatIntervalMillis = workerContext.getContext().getInt(RunpConfigKeys.WORKER_HEARTBEAT_INTERVALMILLIS, 60000);
		
		// create & register message listener
		heartbeatReporter = new HeartbeatReporter(MessageType.WORKER_REGISTRATION, MessageType.HEART_BEAT);
		taskProgressReporter = new TaskProgressReporter(MessageType.TASK_PROGRESS);
		remoteMessageReceiver = new RemoteMessageReceiver(MessageType.TASK_ASSIGNMENT, MessageType.ACK_WORKER_REGISTRATION);
		register(taskProgressReporter);
		register(heartbeatReporter);
		register(remoteMessageReceiver);
		
		// create task launcher instances
		workerContext.getResourceTypes().keySet().stream().forEach(taskType -> {
			Optional<String> c = workerContext.getTaskLauncherClass(taskType);
			c.ifPresent(clazz -> {
				workerContext.getTaskLauncherFactory().registerObject(context, taskType.getCode(), clazz);
			});
		});
	}

	@Override
	public void start() {
		super.start();
		clientConnectionManager = workerContext.getClientConnectionManager();
		
		// try to register to master
		clientConnectionManager.registerToMaster();
		
		// start task launchers
		workerContext.getResourceTypes().keySet().forEach(taskType -> {
			TaskLauncher launcher = workerContext.getTaskLauncherFactory().getObject(taskType.getCode());
			launcher.setTaskProgressReporter(taskProgressReporter);
			launcher.setType(taskType.getCode());
			launcher.start();
		});
		
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
			JSONObject body = JSONObject.parseObject(message.getRpcMessage().getBody());
			Optional<MessageType> messageType = MessageType.fromCode(rpcMessage.getType());
			messageType.ifPresent(mt -> {
				switch(mt) {
					case TASK_ASSIGNMENT:
						LOG.info("Assigned task received: message=" + rpcMessage);
						int taskTypeCode = body.getIntValue(JsonKeys.TASK_TYPE);
						TaskLauncher launcher = workerContext.getTaskLauncherFactory().getObject(taskTypeCode);
						long internalTaskId = rpcMessage.getId();
						launcher.launchTask(internalTaskId, body.getJSONObject(JsonKeys.TASK_PARAMS));
						break;
					case ACK_WORKER_REGISTRATION:
						String status = body.getString(JsonKeys.STATUS);
						if(Status.SUCCEES.toString().equals(status)) {
							masterId = body.getString(JsonKeys.MASTER_ID);
							clientConnectionManager.notifyRegistrationSucceeded();
							LOG.debug("Succeeded to notify dispatcher.");
							LOG.info("Worker registered: id=" + rpcMessage.getId() + ", type=" + rpcMessage.getType() + ", body=" + rpcMessage.getBody());
						} else {
							LOG.info("Worker registration failed: id=" + rpcMessage.getId() + ", type=" + rpcMessage.getType() + ", body=" + rpcMessage.getBody());
							clientConnectionManager.notifyRegistrationFailed();
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
			workerContext.getRpcService().send(message);
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
							workerContext.getRpcService().ask(message);
							LOG.info("Registration message sent: message=" + m);
							break;
						case HEART_BEAT:
							workerContext.getRpcService().send(message);
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
