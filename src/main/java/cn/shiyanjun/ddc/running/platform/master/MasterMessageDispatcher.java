package cn.shiyanjun.ddc.running.platform.master;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;

import cn.shiyanjun.ddc.api.Context;
import cn.shiyanjun.ddc.network.common.AbstractMessageDispatcher;
import cn.shiyanjun.ddc.network.common.LocalMessage;
import cn.shiyanjun.ddc.network.common.RpcMessage;
import cn.shiyanjun.ddc.network.common.RunnableMessageListener;
import cn.shiyanjun.ddc.running.platform.common.WorkerInfo;
import cn.shiyanjun.ddc.running.platform.constants.JsonKeys;
import cn.shiyanjun.ddc.running.platform.constants.MessageType;
import cn.shiyanjun.ddc.running.platform.constants.Status;
import cn.shiyanjun.ddc.running.platform.utils.Time;

public class MasterMessageDispatcher extends AbstractMessageDispatcher {

	private static final Log LOG = LogFactory.getLog(MasterMessageDispatcher.class);
	private final String masterId;
	private final ConcurrentMap<String, WorkerInfo> workers = Maps.newConcurrentMap();
	private final ConcurrentMap<String, Map<String, ResourceData>> resources = Maps.newConcurrentMap();
	
	public MasterMessageDispatcher(String id, Context context) {
		super(context);
		masterId = id;
		register(new WorkerRegistrationReceiver(MessageType.WORKER_REGISTRATION.getCode()));
		register(new HeartbeatReceiver(MessageType.HEART_BEAT.getCode()));
		register(new TaskProgressReceiver(MessageType.TASK_PROGRESS.getCode()));
	}
	
	@Override
	public void start() {
		super.start();
	}
	
	final class WorkerRegistrationReceiver extends RunnableMessageListener<LocalMessage> {

		public WorkerRegistrationReceiver(int messageType) {
			super(messageType);
		}

		@Override
		public void handle(LocalMessage message) {
			final RpcMessage m = message.getRpcMessage();
			assert m.getType() == MessageType.WORKER_REGISTRATION.getCode();
			LOG.info("Worker registration received: " + m);
			
			JSONObject body = JSONObject.parseObject(m.getBody());
			String workerId = body.getString(JsonKeys.WORKER_ID);
			String workerHost = body.getString(JsonKeys.WORKER_HOST);
			WorkerInfo workerInfo = workers.get(workerId);
			if(workerInfo == null) {
				// keep worker information in memory
				workerInfo = new WorkerInfo();
				workerInfo.setId(workerId);
				workerInfo.setHost(workerHost);
				workerInfo.setChannel(message.getChannel());
				workers.putIfAbsent(workerId, workerInfo);
				
				// keep worker resource statuses in memory
				JSONObject resourceTypes = body.getJSONObject(JsonKeys.RESOURCE_TYPES);
				Map<String, ResourceData> rds = resources.get(workerId);
				if(rds == null) {
					rds = Maps.newHashMap();
				}
				for(String type : resourceTypes.keySet()) {
					int capacity = resourceTypes.getIntValue(type);
					ResourceData newResource = new ResourceData(type, capacity);
					ResourceData oldResource = rds.putIfAbsent(type, newResource);
					if(oldResource != null) {
						if(resources.equals(oldResource)) {
							LOG.warn("Resources already registered: wokerId=" + workerId + ", " + newResource);
						} else {
							// update resource statuses
							LOG.info("Resources updated: oldResource=" + oldResource + ", newResource=" + newResource);
						}
					}
				}
				LOG.info("New worker registered: workerId=" + workerId + ", resources=" + resourceTypes);
				
				// reply an successful ack message
				replyRegistration(workerId, m, Status.SUCCEES, "Worker registration succeeded.");
			} else {
				// reply an failed ack message
				replyRegistration(workerId, m, Status.FAILURE, "Worker already registered.");
			}
		}

		private void replyRegistration(String workerId, final RpcMessage m, Status status, String reason) {
			if(m.isNeedReply()) {
				LOG.debug("Registration reply prepareing: receivedMessage=" + m);
				RpcMessage reply = new RpcMessage(m.getId(), MessageType.ACK_WORKER_REGISTRATION.getCode());
				reply.setTimestamp(Time.now());
				JSONObject answer = new JSONObject(true);
				answer.put(JsonKeys.MASTER_ID, masterId);
				answer.put(JsonKeys.STATUS, status);
				answer.put(JsonKeys.REASON, reason);
				reply.setBody(answer.toJSONString());
				LocalMessage ack = new LocalMessage();
				ack.setRpcMessage(reply);
				ack.setFromEndpointId(masterId);
				ack.setToEndpointId(workerId);
				send(ack);
				LOG.info("Registration replied: reliedMessage=" + reply);
			}
		}
		
	}
	
	/**
	 * Process heartbeat messages from <code>Worker</code>.
	 * 
	 * @author yanjun
	 */
	final class HeartbeatReceiver extends RunnableMessageListener<LocalMessage> {
		
		public HeartbeatReceiver(int messageType) {
			super(messageType);
		}

		@Override
		public void handle(LocalMessage message) {
			final RpcMessage m = message.getRpcMessage();
			assert MessageType.HEART_BEAT.getCode() == m.getType();
			LOG.info("Heartbeat received: " + m);
			
			JSONObject body = JSONObject.parseObject(m.getBody());
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
			LOG.info("Worker last contact: " + m);
		}

	}
	
	/**
	 * Receive and handle task progress messages from <code>Worker</code>.
	 * 
	 * @author yanjun
	 */
	final class TaskProgressReceiver extends RunnableMessageListener<LocalMessage> {
		
		public TaskProgressReceiver(int messageType) {
			super(messageType);
		}

		@Override
		public void handle(LocalMessage message) {
			LOG.info("Task progress received: " + message);
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
		
		@Override
		public int hashCode() {
			return 31 * type.hashCode() + 31 * capacity;
		}
		
		@Override
		public boolean equals(Object obj) {
			ResourceData other = (ResourceData) obj;
			return type.equals(other.type) && capacity == other.capacity;
		}
		
		@Override
		public String toString() {
			JSONObject data = new JSONObject(true);
			data.put("type", type);
			data.put("capacity", capacity);
			return data.toJSONString();
		}
	}
	
}
