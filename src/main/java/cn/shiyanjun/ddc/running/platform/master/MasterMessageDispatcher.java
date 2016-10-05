package cn.shiyanjun.ddc.running.platform.master;

import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import cn.shiyanjun.ddc.api.constants.TaskType;
import cn.shiyanjun.ddc.network.common.AbstractMessageDispatcher;
import cn.shiyanjun.ddc.network.common.PeerMessage;
import cn.shiyanjun.ddc.network.common.RpcMessage;
import cn.shiyanjun.ddc.network.common.RunnableMessageListener;
import cn.shiyanjun.ddc.running.platform.common.MasterContext;
import cn.shiyanjun.ddc.running.platform.common.WorkerInfo;
import cn.shiyanjun.ddc.running.platform.constants.JsonKeys;
import cn.shiyanjun.ddc.running.platform.constants.MessageType;
import cn.shiyanjun.ddc.running.platform.constants.Status;
import cn.shiyanjun.ddc.running.platform.utils.Time;

public class MasterMessageDispatcher extends AbstractMessageDispatcher {

	private static final Log LOG = LogFactory.getLog(MasterMessageDispatcher.class);
	private final MasterContext masterContext;
	
	public MasterMessageDispatcher(MasterContext masterContext) {
		super(masterContext.getContext());
		this.masterContext = masterContext;
		register(new WorkerRegistrationReceiver(MessageType.WORKER_REGISTRATION));
		register(new HeartbeatReceiver(MessageType.HEART_BEAT));
	}
	
	@Override
	public void start() {
		super.start();
	}
	
	final class WorkerRegistrationReceiver extends RunnableMessageListener<PeerMessage> {

		public WorkerRegistrationReceiver(MessageType messageType) {
			super(messageType.getCode());
		}

		@Override
		public void handle(PeerMessage message) {
			final RpcMessage rpcMessage = message.getRpcMessage();
			assert rpcMessage.getType() == MessageType.WORKER_REGISTRATION.getCode();
			LOG.info("Worker registration received: " + rpcMessage);
			
			JSONObject body = JSONObject.parseObject(rpcMessage.getBody());
			String workerId = body.getString(JsonKeys.WORKER_ID);
			String workerHost = body.getString(JsonKeys.WORKER_HOST);
			Optional<WorkerInfo> result = masterContext.getWorker(workerId);
			if(!result.isPresent()) {
				// keep worker information in memory
				WorkerInfo workerInfo = new WorkerInfo();
				workerInfo.setId(workerId);
				workerInfo.setHost(workerHost);
				workerInfo.setChannel(message.getChannel());
				masterContext.updateWorker(workerId, workerInfo);
				
				// keep worker resource statuses in memory
				JSONArray resourceTypes = body.getJSONArray(JsonKeys.RESOURCE_TYPES);
				resourceTypes.stream().forEach(o -> {
					JSONObject r = (JSONObject) o;
					String taskTypeDesc = r.getString(JsonKeys.TASK_TYPE_DESC);
					int capacity = r.getIntValue(JsonKeys.CAPACITY);
					TaskType taskType = TaskType.valueOf(taskTypeDesc);
					ResourceData newResource = new ResourceData(taskType, capacity);
					masterContext.updateResource(workerId, newResource);
				});
				LOG.info("New worker registered: workerId=" + workerId + ", resources=" + resourceTypes);
				
				// reply an successful ack message
				replyRegistration(workerId, rpcMessage, Status.SUCCEES, "Worker registration succeeded.");
			} else {
				// reply an failed ack message
				replyRegistration(workerId, rpcMessage, Status.FAILURE, "Worker already registered.");
			}
		}

		private void replyRegistration(String workerId, final RpcMessage m, Status status, String reason) {
			if(m.isNeedReply()) {
				LOG.debug("Registration reply prepareing: receivedMessage=" + m);
				RpcMessage reply = new RpcMessage(m.getId(), MessageType.ACK_WORKER_REGISTRATION.getCode());
				reply.setTimestamp(Time.now());
				JSONObject answer = new JSONObject(true);
				answer.put(JsonKeys.MASTER_ID, masterContext.getThisPeerId());
				answer.put(JsonKeys.STATUS, status);
				answer.put(JsonKeys.REASON, reason);
				reply.setBody(answer.toJSONString());
				PeerMessage ack = new PeerMessage();
				ack.setRpcMessage(reply);
				ack.setFromEndpointId(masterContext.getThisPeerId());
				ack.setToEndpointId(workerId);
				getRpcService().send(ack);
				LOG.info("Registration replied: reliedMessage=" + reply);
			}
		}
		
	}
	
	/**
	 * Process heartbeat messages from <code>Worker</code>.
	 * 
	 * @author yanjun
	 */
	final class HeartbeatReceiver extends RunnableMessageListener<PeerMessage> {
		
		public HeartbeatReceiver(MessageType messageType) {
			super(messageType.getCode());
		}

		@Override
		public void handle(PeerMessage message) {
			final RpcMessage rpcMessage = message.getRpcMessage();
			assert MessageType.HEART_BEAT.getCode() == rpcMessage.getType();
			LOG.debug("Heartbeat received: " + rpcMessage);
			
			JSONObject body = JSONObject.parseObject(rpcMessage.getBody());
			String workerId = body.getString(JsonKeys.WORKER_ID);
			String host = body.getString(JsonKeys.WORKER_HOST);
			Optional<WorkerInfo> result = masterContext.getWorker(workerId);
			if(!result.isPresent()) {
				WorkerInfo workerInfo = new WorkerInfo();
				workerInfo.setId(workerId);
				workerInfo.setHost(host);
				masterContext.updateWorker(workerId, workerInfo);
			}
			result.get().setLastContactTime(Time.now());
			LOG.info("Worker last contact: " + rpcMessage);
		}

	}
	
	public static class ResourceData {
		
		final int taskTypeCode;
		final TaskType taskType;
		final int capacity;
		volatile int freeCount;
		String description;
		final Lock lock = new ReentrantLock();
		
		public ResourceData(TaskType type, int capacity) {
			super();
			this.taskTypeCode = type.getCode();
			this.taskType = type;
			this.capacity = capacity;
			this.freeCount = capacity;
		}
		
		public Lock getLock() {
			return lock;
		}
		
		@Override
		public int hashCode() {
			return 31 * taskType.hashCode() + 31 * capacity;
		}
		
		@Override
		public boolean equals(Object obj) {
			ResourceData other = (ResourceData) obj;
			return taskType.equals(other.taskType) && capacity == other.capacity;
		}
		
		@Override
		public String toString() {
			JSONObject data = new JSONObject(true);
			data.put(JsonKeys.TASK_TYPE_CODE, taskTypeCode);
			data.put(JsonKeys.TASK_TYPE_DESC, taskType);
			data.put(JsonKeys.CAPACITY, capacity);
			return data.toJSONString();
		}

		public int getFreeCount() {
			return freeCount;
		}

		public void incrementFreeCount() {
			this.freeCount++;
		}
		
		public void decrementFreeCount() {
			this.freeCount--;
		}

		public String getDescription() {
			return description;
		}

		public void setDescription(String description) {
			this.description = description;
		}

		public TaskType getTaskType() {
			return taskType;
		}

		public int getCapacity() {
			return capacity;
		}
	}
	
}
