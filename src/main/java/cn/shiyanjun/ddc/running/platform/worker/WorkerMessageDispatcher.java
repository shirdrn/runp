package cn.shiyanjun.ddc.running.platform.worker;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;

import cn.shiyanjun.ddc.api.Context;
import cn.shiyanjun.ddc.network.common.AbstractMessageDispatcher;
import cn.shiyanjun.ddc.network.common.LocalMessage;
import cn.shiyanjun.ddc.network.common.RpcMessage;
import cn.shiyanjun.ddc.network.common.RunnableMessageListener;
import cn.shiyanjun.ddc.running.platform.constants.JsonKeys;
import cn.shiyanjun.ddc.running.platform.constants.MessageType;
import cn.shiyanjun.ddc.running.platform.constants.RunpConfigKeys;
import cn.shiyanjun.ddc.running.platform.constants.Status;

public class WorkerMessageDispatcher extends AbstractMessageDispatcher {

	private static final Log LOG = LogFactory.getLog(WorkerMessageDispatcher.class);
	private final HeartbeatReporter heartbeatReporter;
	private final TaskProgressReporter taskProgressReporter;
	private final RemoteMessageReceiver remoteMessageReceiver;
	private final AtomicLong idGen = new AtomicLong();
	private String masterId;
	private final String workerId;
	private final String workerHost;
	private final int heartbeatIntervalMillis;
	private final Map<String, Integer> resourceTypes = Maps.newHashMap();
	private final BlockingQueue<LocalMessage> taskWaitingQueue = Queues.newLinkedBlockingQueue();
	private final Object registrationLock = new Object();
	private ScheduledExecutorService scheduledExecutorService;
	
	public WorkerMessageDispatcher(Context context) {
		super(context);
		workerId = context.get(RunpConfigKeys.WORKER_ID);
		workerHost = context.get(RunpConfigKeys.WORKER_HOST);
		heartbeatIntervalMillis = context.getInt(RunpConfigKeys.WORKER_HEARTBEAT_INTERVALMILLIS, 60000);
		
		String[] types = context.getStringArray(RunpConfigKeys.RESOURCE_TYPES, null);
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
		final long timeout = 3000L;
		while(true) {
			registerToMaster();
			try {
				synchronized(registrationLock) {
					registrationLock.wait();
				}
				LOG.info("Worker registration succeeded.");
			} catch (InterruptedException e) {
				LOG.warn("Worker registration timeout: timeout=" + timeout);
			}
		}
		
		// send heartbeat to master periodically
//		scheduledExecutorService = Executors.newScheduledThreadPool(1, new NamedThreadFactory("HEARTBEAT"));
//		scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
//			@Override
//			public void run() {
//				RpcMessage hb = new RpcMessage(idGen.incrementAndGet(), MessageType.HEART_BEAT.getCode());
//				JSONObject body = new JSONObject();
//				body.put(JsonKeys.WORKER_ID, workerId);
//				body.put(JsonKeys.WORKER_HOST, workerHost);
//				hb.setBody(body.toJSONString());
//				hb.setTimestamp(System.currentTimeMillis());
//				heartbeatReporter.addMessage(hb);
//				LOG.info("Heartbeart prepared: id=" + hb.getId() + ", body=" + hb.getBody());
//			}
//		}, 3000, heartbeatIntervalMillis, TimeUnit.MILLISECONDS);
	}
	
	private void registerToMaster() {
		RpcMessage rpcMessage = new RpcMessage();
		rpcMessage.setId(System.currentTimeMillis());
		rpcMessage.setType(MessageType.WORKER_REGISTRATION.getCode());
		JSONObject body = new JSONObject(true);
		body.put(JsonKeys.WORKER_ID, workerId);
		body.put(JsonKeys.WORKER_HOST, workerHost);
		JSONObject types = new JSONObject(true);
		types.putAll(resourceTypes);
		body.put(JsonKeys.RESOURCE_TYPES, types);
		rpcMessage.setBody(body.toJSONString());
		rpcMessage.setTimestamp(System.currentTimeMillis());
		
		LocalMessage m = new LocalMessage();
		m.setFromEndpointId(workerId);
		m.setToEndpointId(masterId);
		m.setRpcMessage(rpcMessage);
		heartbeatReporter.addMessage(m);
	}
	
	final class RegistrationSender extends RunnableMessageListener<LocalMessage> {

		public RegistrationSender(int messageType) {
			super(messageType);
		}

		@Override
		public void handle(LocalMessage message) {
			ask(message);
		}

	}
	
	final class RemoteMessageReceiver extends RunnableMessageListener<LocalMessage> {
		
		
		public RemoteMessageReceiver(int... messageType) {
			super(messageType);
		}

		@Override
		public void handle(LocalMessage message) {
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
	final class TaskProgressReporter extends RunnableMessageListener<LocalMessage> {
		
		public TaskProgressReporter(int messageType) {
			super(messageType);
		}

		@Override
		public void handle(LocalMessage message) {
			send(message);
		}

	}
	
	/**
	 * Send heartbeat messages to remote <code>Master</code>.
	 * 
	 * @author yanjun
	 */
	final class HeartbeatReporter extends RunnableMessageListener<LocalMessage> {
		
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
		public void handle(LocalMessage message) {
			send(message);
			LOG.debug("Heartbeart sent: message=" + message);
			if(message.getRpcMessage().getTimestamp() == MessageType.WORKER_REGISTRATION.getCode()) {
				LOG.info("Registration message sent: message=" + message);
			}
		}

	}
	
}
