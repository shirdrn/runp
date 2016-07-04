package cn.shiyanjun.ddc.running.platform;

import cn.shiyanjun.ddc.api.Context;
import cn.shiyanjun.ddc.api.LifecycleAware;
import cn.shiyanjun.ddc.api.common.AbstractComponent;
import cn.shiyanjun.ddc.api.common.ContextImpl;
import cn.shiyanjun.ddc.api.network.MessageListener;
import cn.shiyanjun.ddc.network.NettyRpcClient;
import cn.shiyanjun.ddc.network.common.RpcChannelHandler;
import cn.shiyanjun.ddc.network.common.RpcMessage;
import cn.shiyanjun.ddc.network.common.RpcMessageHandler;
import cn.shiyanjun.ddc.running.platform.slave.WorkerMessageListener;

public class Worker extends AbstractComponent<Context> implements LifecycleAware {

	private LifecycleAware endpoint;
	private final RpcChannelHandler rpcHandler;
	private final MessageListener<RpcMessage> messageListener;
	
	public Worker(Context context) {
		super(context);
		rpcHandler = new RpcMessageHandler(context);
		messageListener = new WorkerMessageListener(context, rpcHandler);
		rpcHandler.setMessageListener(messageListener);
	}
	
	@Override
	public void start() {
		endpoint = NettyRpcClient.newClient(context, rpcHandler, messageListener);
		endpoint.start();		
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
