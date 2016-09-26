package cn.shiyanjun.ddc.running.platform.worker;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONObject;
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
	private final RegistrationSender registrationSender;
	private final HeartbeatReporter heartbeatReporter;
	private final TaskProgressReporter taskProgressReporter;
	private final TaskAssingmentReceiver taskAssingmentReceiver;
	private final AtomicLong idGen = new AtomicLong();
	private final String workerId;
	private final String workerHost;
	private final int heartbeatIntervalMillis;
	
	private final BlockingQueue<RpcMessage> taskWaitingQueue = Queues.newLinkedBlockingQueue();
	
	public WorkerMessageDispatcher(Context context) {
		super(context);
		workerId = context.get(RunpConfigKeys.WORKER_ID);
		workerHost = context.get(RunpConfigKeys.WORKER_HOST);
		heartbeatIntervalMillis = context.getInt(RunpConfigKeys.WORKER_HEARTBEAT_INTERVALMILLIS, 60000);
		registrationSender = new RegistrationSender(MessageType.WORKER_REGISTRATION.getCode());
		heartbeatReporter = new HeartbeatReporter(MessageType.HEART_BEAT.getCode());
		taskProgressReporter = new TaskProgressReporter(MessageType.TASK_PROGRESS.getCode());
		taskAssingmentReceiver = new TaskAssingmentReceiver(MessageType.TASK_ASSIGNMENT.getCode());
		
		register(registrationSender);
		register(taskProgressReporter);
		register(heartbeatReporter);
		register(taskAssingmentReceiver);
	}
	
	@Override
	public void start() {
		super.start();
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
	
	final class TaskAssingmentReceiver extends RunnableMessageListener<RpcMessage> {
		
		public TaskAssingmentReceiver(int messageType) {
			super(messageType);
		}

		@Override
		public void handle(RpcMessage message) {
			taskWaitingQueue.add(message);
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
		
		private ScheduledExecutorService scheduledExecutorService;
		
		public HeartbeatReporter(int messageType) {
			super(messageType);
		}
		
		@Override
		public void start() {
			super.start();
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
		
		@Override
		public void stop() {
			super.stop();
			scheduledExecutorService.shutdown();
		}

		@Override
		public void handle(RpcMessage message) {
			ask(message);
			LOG.info("Heartbeart sent: id=" + message.getId() + ", body=" + message.getBody());
		}

	}
	
}
