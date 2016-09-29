package cn.shiyanjun.ddc.running.platform;

import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Preconditions;

import cn.shiyanjun.ddc.api.Context;
import cn.shiyanjun.ddc.api.LifecycleAware;
import cn.shiyanjun.ddc.api.common.AbstractComponent;
import cn.shiyanjun.ddc.api.common.ContextImpl;
import cn.shiyanjun.ddc.network.NettyRpcClient;
import cn.shiyanjun.ddc.network.common.MessageDispatcher;
import cn.shiyanjun.ddc.network.common.NettyRpcEndpoint;
import cn.shiyanjun.ddc.network.common.RpcMessageHandler;
import cn.shiyanjun.ddc.running.platform.common.EndpointThread;
import cn.shiyanjun.ddc.running.platform.constants.RunpConfigKeys;
import cn.shiyanjun.ddc.running.platform.worker.WorkerMessageDispatcher;
import cn.shiyanjun.ddc.running.platform.worker.WorkerRpcMessageHandler;

public class Worker extends AbstractComponent implements LifecycleAware {

	private static final Log LOG = LogFactory.getLog(Worker.class);
	private NettyRpcEndpoint endpoint;
	private final RpcMessageHandler rpcMessageHandler;
	private final MessageDispatcher dispatcher;
	private final String workerId;
	private final String workerHost;
	private final EndpointThread endpointThread;
	
	public Worker(Context context) {
		super(context);
		dispatcher = new WorkerMessageDispatcher(context);
		rpcMessageHandler = new WorkerRpcMessageHandler(context, dispatcher);
		dispatcher.setRpcMessageHandler(rpcMessageHandler);
		
		workerId = context.get(RunpConfigKeys.WORKER_ID, UUID.randomUUID().toString());
		workerHost = context.get(RunpConfigKeys.WORKER_HOST);
		Preconditions.checkArgument(workerId != null);
		Preconditions.checkArgument(workerHost != null);
		endpoint = NettyRpcEndpoint.newEndpoint(NettyRpcClient.class, context, rpcMessageHandler);
		endpointThread = new EndpointThread();
	}
	
	@Override
	public void start() {
		try {
			endpointThread.setEndpoint(endpoint);
			new Thread(endpointThread).start();
			Thread.sleep(3000);
			
			dispatcher.start();
			LOG.info("Worker started.");
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
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
