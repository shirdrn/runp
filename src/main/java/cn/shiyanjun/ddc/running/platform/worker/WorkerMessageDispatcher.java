package cn.shiyanjun.ddc.running.platform.worker;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;

import cn.shiyanjun.ddc.api.utils.NamedThreadFactory;
import cn.shiyanjun.ddc.network.common.AbstractMessageDispatcher;
import cn.shiyanjun.ddc.network.common.PeerMessage;
import cn.shiyanjun.ddc.network.common.RpcMessage;
import cn.shiyanjun.ddc.network.common.RunnableMessageListener;
import cn.shiyanjun.ddc.running.platform.common.RunpContext;
import cn.shiyanjun.ddc.running.platform.constants.JsonKeys;
import cn.shiyanjun.ddc.running.platform.constants.MessageType;
import cn.shiyanjun.ddc.running.platform.constants.RunpConfigKeys;
import cn.shiyanjun.ddc.running.platform.constants.Status;
import cn.shiyanjun.ddc.running.platform.utils.Time;

public class WorkerMessageDispatcher extends AbstractMessageDispatcher {

	private static final Log LOG = LogFactory.getLog(WorkerMessageDispatcher.class);
	private final HeartbeatReporter heartbeatReporter;
	private final TaskProgressReporter taskProgressReporter;
	private final RemoteMessageReceiver remoteMessageReceiver;
	private final AtomicLong idGen = new AtomicLong();
	private final RunpContext context;
	private final String workerId;
	private volatile String masterId;
	private final String workerHost;
	private final int heartbeatIntervalMillis;
	private final Map<String, Integer> resourceTypes = Maps.newHashMap();
	private final BlockingQueue<PeerMessage> taskWaitingQueue = Queues.newLinkedBlockingQueue();
	private final Object registrationLock = new Object();
	private ScheduledExecutorService scheduledExecutorService;
	
	public WorkerMessageDispatcher(RunpContext context) {
		super(context.getContext());
		this.context = context;
		masterId = context.getMasterId();
		workerId = context.getThisPeerId();
		workerHost = context.getContext().get(RunpConfigKeys.WORKER_HOST);
		heartbeatIntervalMillis = context.getContext().getInt(RunpConfigKeys.WORKER_HEARTBEAT_INTERVALMILLIS, 60000);
		
		String[] types = context.getContext().getStringArray(RunpConfigKeys.RESOURCE_TYPES, null);
		Preconditions.checkArgument(types != null, "Configured resource types shouldn't be null");
		
		for(String t : types) {
			String[] type = t.split(":");
			String typeCode = type[0];
			String typeValue = type[1];
			resourceTypes.put(typeCode, Integer.parseInt(typeValue));
			LOG.info("Resource supported: typeCode=" + typeCode + ", typeValue=" + typeValue);
		}
		
		heartbeatReporter = new HeartbeatReporter(
				MessageType.WORKER_REGISTRATION.getCode(), 
				MessageType.HEART_BEAT.getCode());
		taskProgressReporter = new TaskProgressReporter(MessageType.TASK_PROGRESS.getCode());
		remoteMessageReceiver = new RemoteMessageReceiver(
				MessageType.TASK_ASSIGNMENT.getCode(), 
				MessageType.ACK_WORKER_REGISTRATION.getCode());
		
		register(taskProgressReporter);
		register(heartbeatReporter);
		register(remoteMessageReceiver);
	}
	
	@Override
	public void start() {
		super.start();
		
		// try to register to master
		while(true) {
			registerToMaster();
			try {
				synchronized(registrationLock) {
					registrationLock.wait();
				}
				LOG.info("Worker registration succeeded.");
				break;
			} catch (InterruptedException e) {
				LOG.warn("Worker registration interrupet.");
			}
		}
		
		// send heartbeat to master periodically
		scheduledExecutorService = Executors.newScheduledThreadPool(1, new NamedThreadFactory("HEARTBEAT"));
		scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				PeerMessage message = prepareHeartbeatMessage();
				heartbeatReporter.addMessage(message);
				LOG.info("Heartbeart prepared: id=" + message.getRpcMessage().getId() + ", body=" + message.getRpcMessage().getBody());
			}
			
			private PeerMessage prepareHeartbeatMessage() {
				RpcMessage hb = new RpcMessage(idGen.incrementAndGet(), MessageType.HEART_BEAT.getCode());
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
				message.setChannel(context.getChannel(masterId));
				return message;
			}
			
		}, 3000, heartbeatIntervalMillis, TimeUnit.MILLISECONDS);
	}
	
	private void registerToMaster() {
		RpcMessage rpcMessage = new RpcMessage();
		rpcMessage.setId(idGen.incrementAndGet());
		rpcMessage.setType(MessageType.WORKER_REGISTRATION.getCode());
		rpcMessage.setNeedReply(true);
		
		JSONObject body = new JSONObject(true);
		body.put(JsonKeys.WORKER_ID, workerId);
		body.put(JsonKeys.WORKER_HOST, workerHost);
		JSONObject types = new JSONObject(true);
		types.putAll(resourceTypes);
		body.put(JsonKeys.RESOURCE_TYPES, types);
		rpcMessage.setBody(body.toJSONString());
		rpcMessage.setTimestamp(Time.now());
		
		PeerMessage m = new PeerMessage();
		m.setFromEndpointId(workerId);
		m.setToEndpointId(masterId);
		m.setRpcMessage(rpcMessage);
		m.setChannel(context.getChannel(masterId));
		heartbeatReporter.addMessage(m);
	}
	
	final class RegistrationSender extends RunnableMessageListener<PeerMessage> {

		public RegistrationSender(int messageType) {
			super(messageType);
		}

		@Override
		public void handle(PeerMessage message) {
			getRpcService().ask(message);
		}

	}
	
	final class RemoteMessageReceiver extends RunnableMessageListener<PeerMessage> {
		
		
		public RemoteMessageReceiver(int... messageType) {
			super(messageType);
		}

		@Override
		public void handle(PeerMessage message) {
			RpcMessage m = message.getRpcMessage();
			Optional<MessageType> messageType = MessageType.fromCode(m.getType());
			if(messageType.isPresent()) {
				switch(messageType.get()) {
					case TASK_ASSIGNMENT:
						taskWaitingQueue.add(message);
						break;
					case ACK_WORKER_REGISTRATION:
						synchronized(registrationLock) {
							JSONObject repliedAck = JSONObject.parseObject(message.getRpcMessage().getBody());
							String status = repliedAck.getString(JsonKeys.STATUS);
							if(Status.SUCCEES.toString().equals(status)) {
								masterId = repliedAck.getString(JsonKeys.MASTER_ID);
								registrationLock.notify();
								LOG.debug("Notify dispatcher registration succeeded.");
								LOG.info("Worker registered: id=" + m.getId() + ", type=" + m.getType() + ", body=" + m.getBody());
							} else {
								LOG.info("Worker registration failed: id=" + m.getId() + ", type=" + m.getType() + ", body=" + m.getBody());
								// register to master again
								registerToMaster();
							}
						}
						break;
					default:
				}
			}
		}

	}
	
	/**
	 * Send task progress report messages to remote <code>Master</code>.
	 * 
	 * @author yanjun
	 */
	final class TaskProgressReporter extends RunnableMessageListener<PeerMessage> {
		
		public TaskProgressReporter(int messageType) {
			super(messageType);
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
		
		public HeartbeatReporter(int... messageType) {
			super(messageType);
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
