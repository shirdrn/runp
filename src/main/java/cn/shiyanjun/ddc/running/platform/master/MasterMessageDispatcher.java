package cn.shiyanjun.ddc.running.platform.master;

import java.util.concurrent.ConcurrentMap;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;

import cn.shiyanjun.ddc.api.Context;
import cn.shiyanjun.ddc.network.common.AbstractMessageDispatcher;
import cn.shiyanjun.ddc.network.common.RpcMessage;
import cn.shiyanjun.ddc.network.common.RunnableMessageListener;
import cn.shiyanjun.ddc.running.platform.common.WorkerInfo;
import cn.shiyanjun.ddc.running.platform.constants.JsonKeys;
import cn.shiyanjun.ddc.running.platform.constants.MessageType;

public class MasterMessageDispatcher extends AbstractMessageDispatcher {

	private final ConcurrentMap<String, WorkerInfo> workers = Maps.newConcurrentMap();
	private final ConcurrentMap<String, ResourceData> resources = Maps.newConcurrentMap();
	
	public MasterMessageDispatcher(Context context) {
		super(context);
		register(new WorkerRegistrationReceiver(MessageType.WORKER_REGISTRATION.getCode()));
		register(new HeartbeatReceiver(MessageType.HEART_BEAT.getCode()));
		register(new TaskProgressReceiver(MessageType.TASK_PROGRESS.getCode()));
	}
	
	@Override
	public void start() {
		super.start();
	}
	
	final class WorkerRegistrationReceiver extends RunnableMessageListener<RpcMessage> {

		public WorkerRegistrationReceiver(int messageType) {
			super(messageType);
		}

		@Override
		public void handle(RpcMessage message) {
			assert message.getType() == MessageType.WORKER_REGISTRATION.getCode();
			JSONObject body = JSONObject.parseObject(message.getBody());
			String workerId = body.getString(JsonKeys.WORKER_ID);
			JSONObject resourceTypes = body.getJSONObject(JsonKeys.RESOURCE_TYPES);
			for(String type : resourceTypes.keySet()) {
				int capacity = resourceTypes.getIntValue(type);
				ResourceData resource = new ResourceData(type, capacity);
				ResourceData oldResource = resources.putIfAbsent(workerId, resource);
				if(oldResource != null) {
					
				}
			}
		}
		
	}
	
	/**
	 * Process heartbeat messages from <code>Worker</code>.
	 * 
	 * @author yanjun
	 */
	final class HeartbeatReceiver extends RunnableMessageListener<RpcMessage> {
		
		public HeartbeatReceiver(int messageType) {
			super(messageType);
		}

		@Override
		public void handle(RpcMessage message) {
			assert MessageType.HEART_BEAT.getCode() == message.getType();
			JSONObject body = JSONObject.parseObject(message.getBody());
			String workerId = body.getString(JsonKeys.WORKER_ID);
			String host = body.getString(JsonKeys.WORKER_HOST);
			WorkerInfo info = workers.get(workerId);
			if(info == null) {
				info = new WorkerInfo();
				info.setId(workerId);
				info.setHost(host);
				workers.putIfAbsent(workerId, info);
			}
			info.setLastContatTime(System.currentTimeMillis());
		}

	}
	
	/**
	 * Receive and handle task progress messages from <code>Worker</code>.
	 * 
	 * @author yanjun
	 */
	final class TaskProgressReceiver extends RunnableMessageListener<RpcMessage> {
		
		public TaskProgressReceiver(int messageType) {
			super(messageType);
		}

		@Override
		public void handle(RpcMessage message) {
			
		}

	}
	
	class ResourceData {
		
		final String type;
		final int capacity;
		volatile int freeCount;
		String description;
		
		public ResourceData(String type, int capacity) {
			super();
			this.type = type;
			this.capacity = capacity;
		}
	}
	
}
