package cn.shiyanjun.ddc.running.platform;

import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import cn.shiyanjun.ddc.api.Context;
import cn.shiyanjun.ddc.api.LifecycleAware;
import cn.shiyanjun.ddc.api.common.AbstractComponent;
import cn.shiyanjun.ddc.api.common.ContextImpl;
import cn.shiyanjun.ddc.network.NettyRpcClient;
import cn.shiyanjun.ddc.network.common.MessageDispatcher;
import cn.shiyanjun.ddc.network.common.NettyRpcEndpoint;
import cn.shiyanjun.ddc.network.common.RpcMessage;
import cn.shiyanjun.ddc.network.common.RpcMessageHandler;
import cn.shiyanjun.ddc.network.common.RunnableMessageListener;
import cn.shiyanjun.ddc.running.platform.constants.RunpConfigKeys;
import cn.shiyanjun.ddc.running.platform.constants.JsonKeys;
import cn.shiyanjun.ddc.running.platform.constants.MessageType;
import cn.shiyanjun.ddc.running.platform.master.MasterMessageDispatcher;

public class Worker extends AbstractComponent implements LifecycleAware {

	private static final Log LOG = LogFactory.getLog(Worker.class);
	private LifecycleAware endpoint;
	private final RpcMessageHandler rpcMessageHandler;
	private final MessageDispatcher dispatcher;
	private RunnableMessageListener<RpcMessage> registrationListener;
	private final Map<String, Integer> resourceTypes = Maps.newHashMap();
	private final String workerId;
	private final String workerHost;
	
	public Worker(Context context) {
		super(context);
		dispatcher = new MasterMessageDispatcher(context);
		rpcMessageHandler = new RpcMessageHandler(context, dispatcher);
		dispatcher.setRpcMessageHandler(rpcMessageHandler);
		
		workerId = context.get(RunpConfigKeys.WORKER_ID, UUID.randomUUID().toString());
		workerHost = context.get(RunpConfigKeys.WORKER_HOST);
		Preconditions.checkArgument(workerId != null);
		Preconditions.checkArgument(workerHost != null);
		
		String[] types = context.getStringArray(RunpConfigKeys.RESOURCE_TYPES, null);
		Preconditions.checkArgument(types != null, "Configured resource types shouldn't be null");
		for(String t : types) {
			String[] type = t.split(":");
			String typeCode = type[0];
			String typeValue = type[1];
			resourceTypes.put(typeCode, Integer.parseInt(typeValue));
			LOG.info("Resource supported: typeCode=" + typeCode + ", typeValue=" + typeValue);
		}
	}
	
	@Override
	public void start() {
		endpoint = NettyRpcEndpoint.newEndpoint(NettyRpcClient.class, context, rpcMessageHandler);
		endpoint.start();
		dispatcher.start();
		LOG.info("Worker started.");
		
		registrationListener = dispatcher.getMessageListener(MessageType.WORKER_REGISTRATION.getCode());
		// try to register to master
		for (int i = 0; i < 3; i++) {
			try {
				registerToMaster();
				Thread.sleep(1000);
			} catch (Exception e) {
				LOG.warn("Fail to register to master: ", e);
			}
		}
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
		registrationListener.addMessage(message);
	}

	@Override
	public void stop() {
		endpoint.stop();		
	}
	
	public static void main(String[] args) {
		final Context context = new ContextImpl();
		Worker worker = new Worker(context);
		worker.start();		
	}

}
