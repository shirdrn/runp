package cn.shiyanjun.running.platform.master;

import java.util.Optional;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import cn.shiyanjun.platform.api.constants.TaskType;
import cn.shiyanjun.platform.network.common.AbstractMessageDispatcher;
import cn.shiyanjun.platform.network.common.PeerMessage;
import cn.shiyanjun.platform.network.common.RpcMessage;
import cn.shiyanjun.platform.network.common.RunnableMessageListener;
import cn.shiyanjun.running.platform.constants.JsonKeys;
import cn.shiyanjun.running.platform.constants.MessageType;
import cn.shiyanjun.running.platform.constants.Status;
import cn.shiyanjun.running.platform.utils.Time;

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
				
				// reply a successful ack message
				replyRegistration(workerId, rpcMessage, Status.SUCCEES, "Worker registration succeeded.");
			} else {
				// reply a failed ack message
				replyRegistration(workerId, rpcMessage, Status.FAILURE, "Worker already registered.");
			}
		}

		private void replyRegistration(String workerId, final RpcMessage rpcMessage, Status status, String reason) {
			if(rpcMessage.isNeedReply()) {
				LOG.debug("Registration reply prepareing: receivedMessage=" + rpcMessage);
				RpcMessage repliedMessage = new RpcMessage(rpcMessage.getId(), MessageType.ACK_WORKER_REGISTRATION.getCode());
				repliedMessage.setTimestamp(Time.now());
				JSONObject answer = new JSONObject(true);
				answer.put(JsonKeys.MASTER_ID, masterContext.getPeerId());
				answer.put(JsonKeys.STATUS, status);
				answer.put(JsonKeys.REASON, reason);
				repliedMessage.setBody(answer.toJSONString());
				
				PeerMessage ack = new PeerMessage();
				ack.setRpcMessage(repliedMessage);
				ack.setFromEndpointId(masterContext.getPeerId());
				ack.setToEndpointId(workerId);
				masterContext.getRpcService().send(ack);
				LOG.info("Registration replied: repliedMessage=" + repliedMessage);
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
	
}
