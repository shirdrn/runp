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

import cn.shiyanjun.ddc.api.Context;
import cn.shiyanjun.ddc.api.utils.NamedThreadFactory;
import cn.shiyanjun.ddc.network.common.AbstractMessageDispatcher;
import cn.shiyanjun.ddc.network.common.RpcMessage;
import cn.shiyanjun.ddc.network.common.RunnableMessageListener;
import cn.shiyanjun.ddc.running.platform.constants.JsonKeys;
import cn.shiyanjun.ddc.running.platform.constants.MessageType;
import cn.shiyanjun.ddc.running.platform.constants.RunpConfigKeys;

public class WorkerMessageDispatcher extends AbstractMessageDispatcher {

	private static final Log LOG = LogFactory.getLog(WorkerMessageDispatcher.class);
	private final HeartbeatReporter heartbeatReporter;
	private final TaskProgressReporter taskProgressReporter;
	private final RemoteMessageReceiver remoteMessageReceiver;
	private final AtomicLong idGen = new AtomicLong();
	private final String workerId;
	private final String workerHost;
	private final int heartbeatIntervalMillis;
	private final Map<String, Integer> resourceTypes = Maps.newHashMap();
	private final BlockingQueue<RpcMessage> taskWaitingQueue = Queues.newLinkedBlockingQueue();
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
					registrationLock.wait(timeout);
				}
			} catch (InterruptedException e) {
				LOG.warn("Worker registration timeout: timeout=" + timeout);
			}
			break;
		}
		
		// send heartbeat to master periodically
		scheduledExecutorService = Executors.newScheduledThreadPool(1, new NamedThreadFactory("HEARTBEAT"));
		scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				RpcMessage hb = new RpcMessage(idGen.incrementAndGet(), MessageType.HEART_BEAT.getCode());
				JSONObject body = new JSONObject();
				body.put(JsonKeys.WORKER_ID, workerId);
				body.put(JsonKeys.WORKER_HOST, workerHost);
				hb.setBody(body.toJSONString());
				hb.setTimestamp(System.currentTimeMillis());
				heartbeatReporter.addMessage(hb);
				LOG.info("Heartbeart prepared: id=" + hb.getId() + ", body=" + hb.getBody());
			}
		}, 3000, heartbeatIntervalMillis, TimeUnit.MILLISECONDS);
	}
	
	private void registerToMaster() {
		RpcMessage message = new RpcMessage();
		message.setId(System.currentTimeMillis());
		message.setType(MessageType.WORKER_REGISTRATION.getCode());
		JSONObject body = new JSONObject(true);
		body.put(JsonKeys.WORKER_ID, workerId);
		body.put(JsonKeys.WORKER_HOST, workerHost);
		JSONObject types = new JSONObject(true);
		types.putAll(resourceTypes);
		body.put(JsonKeys.RESOURCE_TYPES, types);
		message.setBody(body.toJSONString());
		message.setTimestamp(System.currentTimeMillis());
		heartbeatReporter.addMessage(message);
	}
	
	private void ask(RpcMessage message) {
		getRpcMessageHandler().ask(message);
	}
	
	final class RegistrationSender extends RunnableMessageListener<RpcMessage> {

		public RegistrationSender(int messageType) {
			super(messageType);
		}

		@Override
		public void handle(RpcMessage message) {
			ask(message);
		}

	}
	
	final class RemoteMessageReceiver extends RunnableMessageListener<RpcMessage> {
		
		
		public RemoteMessageReceiver(int... messageType) {
			super(messageType);
		}

		@Override
		public void handle(RpcMessage message) {
			Optional<MessageType> messageType = MessageType.fromCode(message.getType());
			if(messageType.isPresent()) {
				switch(messageType.get()) {
					case TASK_ASSIGNMENT:
						taskWaitingQueue.add(message);
						break;
					case ACK_WORKER_REGISTRATION:
						synchronized(registrationLock) {
							registrationLock.notify();
						}
						LOG.info("Worker registered: id=" + message.getId() + ", type=" + message.getType() + ", body=" + message.getBody());
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
	final class TaskProgressReporter extends RunnableMessageListener<RpcMessage> {
		
		public TaskProgressReporter(int messageType) {
			super(messageType);
		}

		@Override
		public void handle(RpcMessage message) {
			ask(message);
		}

	}
	
	/**
	 * Send heartbeat messages to remote <code>Master</code>.
	 * 
	 * @author yanjun
	 */
	final class HeartbeatReporter extends RunnableMessageListener<RpcMessage> {
		
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
		public void handle(RpcMessage message) {
			ask(message);
			LOG.info("Heartbeart sent: id=" + message.getId() + ", body=" + message.getBody());
		}

	}
	
}
