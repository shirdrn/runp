package cn.shiyanjun.ddc.running.platform.master;

import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.Maps;

import cn.shiyanjun.ddc.api.Context;
import cn.shiyanjun.ddc.network.common.AbstractMessageDispatcher;
import cn.shiyanjun.ddc.network.common.RpcMessage;
import cn.shiyanjun.ddc.network.common.RunnableMessageListener;
import cn.shiyanjun.ddc.running.platform.common.WorkerInfo;
import cn.shiyanjun.ddc.running.platform.constants.MessageType;

public class MasterMessageDispatcher extends AbstractMessageDispatcher {

	private final ConcurrentMap<String, WorkerInfo> workers = Maps.newConcurrentMap();
	
	public MasterMessageDispatcher(Context context) {
		super(context);
		register(new WorkerRegistrationProcessor(MessageType.WORKER_REGISTRATION.getCode()));
		register(new HeartbeatProcessor(MessageType.HEART_BEAT.getCode()));
		register(new TaskProgressProcessor(MessageType.TASK_PROGRESS.getCode()));
		register(new WorkerRegistrationProcessor(MessageType.RESOURCE_REPORT.getCode()));
	}
	
	@Override
	public void start() {
		super.start();
	}
	
	final class WorkerRegistrationProcessor extends RunnableMessageListener<RpcMessage> {

		public WorkerRegistrationProcessor(int messageType) {
			super(messageType);
		}

		@Override
		public void handle(RpcMessage message) {
			// TODO Auto-generated method stub
			
		}
		
	}
	
	final class ResourceReportProcessor extends RunnableMessageListener<RpcMessage> {

		public ResourceReportProcessor(int messageType) {
			super(messageType);
		}

		@Override
		public void handle(RpcMessage message) {
			// TODO Auto-generated method stub
			
		}
		
	}
	
	/**
	 * Process heartbeat messages from <code>Worker</code>.
	 * 
	 * @author yanjun
	 */
	final class HeartbeatProcessor extends RunnableMessageListener<RpcMessage> {
		
		public HeartbeatProcessor(int messageType) {
			super(messageType);
		}

		@Override
		public void handle(RpcMessage message) {
			
		}

	}
	
	/**
	 * Receive and handle task progress messages from <code>Worker</code>.
	 * 
	 * @author yanjun
	 */
	final class TaskProgressProcessor extends RunnableMessageListener<RpcMessage> {
		
		public TaskProgressProcessor(int messageType) {
			super(messageType);
		}

		@Override
		public void handle(RpcMessage message) {
			
		}

	}
	
}
