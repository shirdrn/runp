package cn.shiyanjun.ddc.running.platform;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.shiyanjun.ddc.api.Context;
import cn.shiyanjun.ddc.api.LifecycleAware;
import cn.shiyanjun.ddc.api.common.AbstractComponent;
import cn.shiyanjun.ddc.api.common.ContextImpl;
import cn.shiyanjun.ddc.network.NettyRpcClient;
import cn.shiyanjun.ddc.network.common.MessageDispatcher;
import cn.shiyanjun.ddc.network.common.RpcMessageHandler;
import cn.shiyanjun.ddc.running.platform.master.MasterMessageDispatcher;

public class Worker extends AbstractComponent implements LifecycleAware {

	private static final Log LOG = LogFactory.getLog(Worker.class);
	private LifecycleAware endpoint;
	private final RpcMessageHandler rpcMessageHandler;
	private final MessageDispatcher dispatcher;
	
	public Worker(Context context) {
		super(context);
		dispatcher = new MasterMessageDispatcher(context);
		rpcMessageHandler = new RpcMessageHandler(context, dispatcher);
		dispatcher.setRpcMessageHandler(rpcMessageHandler);
	}
	
	@Override
	public void start() {
		endpoint = NettyRpcClient.newClient(getContext(), rpcMessageHandler);
		endpoint.start();
		
		dispatcher.start();
		LOG.info("Worker started.");
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
