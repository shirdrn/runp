package cn.shiyanjun.ddc.running.platform.slave;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONObject;

import cn.shiyanjun.ddc.api.Context;
import cn.shiyanjun.ddc.api.constants.ConfigKeys;
import cn.shiyanjun.ddc.api.utils.NamedThreadFactory;
import cn.shiyanjun.ddc.network.common.AbstractMessageDispatcher;
import cn.shiyanjun.ddc.network.common.RpcMessage;
import cn.shiyanjun.ddc.network.common.RunnableMessageListener;
import cn.shiyanjun.ddc.running.platform.constants.JsonKeys;
import cn.shiyanjun.ddc.running.platform.constants.MessageType;

public class WorkerMessageDispatcher extends AbstractMessageDispatcher {

	private static final Log LOG = LogFactory.getLog(WorkerMessageDispatcher.class);
	private final HeartbeatReporter heartbeatReporter;
	private final ResourceReporter resourceReporter;
	private final TaskProgressReporter taskProgressReporter;
	private final TaskAssingmentProcessor taskAssingmentProcessor;
	
	public WorkerMessageDispatcher(Context context) {
		super(context);
		heartbeatReporter = new HeartbeatReporter(MessageType.HEART_BEAT.getCode());
		resourceReporter = new ResourceReporter(MessageType.RESOURCE_REPORT.getCode());
		taskProgressReporter = new TaskProgressReporter(MessageType.TASK_PROGRESS.getCode());
		taskAssingmentProcessor = new TaskAssingmentProcessor(MessageType.TASK_ASSIGNMENT.getCode());
		
		register(resourceReporter);
		register(taskProgressReporter);
		register(heartbeatReporter);
		register(taskAssingmentProcessor);
	}
	
	@Override
	public void start() {
		super.start();
	}
	
	final class TaskAssingmentProcessor extends RunnableMessageListener<RpcMessage> {
		
		public TaskAssingmentProcessor(int messageType) {
			super(messageType);
		}

		@Override
		public void handle(RpcMessage message) {
			
		}

	}
	
	/**
	 * Send resource report messages to remote <code>Master</code>.
	 * 
	 * @author yanjun
	 */
	final class ResourceReporter extends RunnableMessageListener<RpcMessage> {
		
		public ResourceReporter(int messageType) {
			super(messageType);
		}

		@Override
		public void handle(RpcMessage message) {
			
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
			int period = context.getInt(ConfigKeys.RPC_HEARTBEAT_INTERVAL, 5000);
			scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

				@Override
				public void run() {
					RpcMessage hb = new RpcMessage(System.currentTimeMillis(), MessageType.HEART_BEAT.getCode());
					JSONObject body = new JSONObject();
					body.put(JsonKeys.HOST, "10.10.0.123");
					hb.setBody(body.toJSONString());
					hb.setTimestamp(System.currentTimeMillis());
					addMessage(hb);
					LOG.info("Heartbeart sent: id=" + hb.getId() + ", body=" + hb.getBody());
				}
				
			}, 1, period, TimeUnit.MILLISECONDS);
		}
		
		@Override
		public void stop() {
			super.stop();
			scheduledExecutorService.shutdown();
		}

		@Override
		public void handle(RpcMessage message) {
			getRpcMessageHandler().ask(message);
		}

	}
	
}
