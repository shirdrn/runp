package cn.shiyanjun.running.platform.component.worker;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import cn.shiyanjun.platform.network.api.MessageDispatcher;
import cn.shiyanjun.platform.network.common.PeerMessage;
import cn.shiyanjun.platform.network.common.RpcMessage;
import cn.shiyanjun.platform.network.common.RunnableMessageListener;
import cn.shiyanjun.running.platform.common.AbstractConnectionManager;
import cn.shiyanjun.running.platform.constants.JsonKeys;
import cn.shiyanjun.running.platform.constants.MessageType;
import cn.shiyanjun.running.platform.utils.Time;

public class ClientConnectionManager extends AbstractConnectionManager {

	private static final Log LOG = LogFactory.getLog(ClientConnectionManager.class);
	private final WorkerContext workerContext;
	private final RunnableMessageListener<PeerMessage> heartbeatReporter;
	private final AtomicLong messageIdGenerator;
	private final Object registrationLock = new Object();
	
	public ClientConnectionManager(WorkerContext workerContext, AtomicLong idGen, MessageDispatcher dispatcher) {
		super(workerContext);
		this.workerContext = workerContext;
		this.heartbeatReporter = dispatcher.getMessageListener(MessageType.HEART_BEAT.getCode());
		messageIdGenerator = idGen;
	}
	
	public void registerToMaster() {
		while(true) {
			String workerId = workerContext.getPeerId();
			try {
				RpcMessage rpcMessage = new RpcMessage();
				rpcMessage.setId(messageIdGenerator.incrementAndGet());
				rpcMessage.setType(MessageType.WORKER_REGISTRATION.getCode());
				rpcMessage.setNeedReply(true);
				
				JSONObject body = new JSONObject(true);
				body.put(JsonKeys.WORKER_ID, workerId);
				body.put(JsonKeys.WORKER_HOST, workerContext.getWorkerHost());
				JSONArray types = new JSONArray();
				workerContext.getResourceTypes().keySet().stream().forEach(tp -> {
					JSONObject res = new JSONObject(true);
					res.put(JsonKeys.TASK_TYPE, tp.getCode());
					res.put(JsonKeys.TASK_TYPE_DESC, tp);
					res.put(JsonKeys.CAPACITY, workerContext.getResourceTypes().get(tp));
					types.add(res);
				});
				
				body.put(JsonKeys.RESOURCE_TYPES, types);
				rpcMessage.setBody(body.toJSONString());
				rpcMessage.setTimestamp(Time.now());
				
				PeerMessage m = new PeerMessage();
				m.setFromEndpointId(workerId);
				m.setToEndpointId(workerContext.getMasterId());
				m.setRpcMessage(rpcMessage);
				m.setChannel(workerContext.getChannel(workerContext.getMasterId()));
				heartbeatReporter.addMessage(m);
				
				synchronized(registrationLock) {
					registrationLock.wait();
				}
				break;
			} catch (InterruptedException e) {
				LOG.warn("Worker registration interrupted.");
			}
		}
	}
	
	public void notifyRegistrationSucceeded() {
		synchronized(registrationLock) {
			registrationLock.notify();
		}
	}
	
	public void notifyRegistrationFailed() {
		synchronized(registrationLock) {
			registrationLock.notify();
		}
		// register to master again
		registerToMaster();
	}

}
