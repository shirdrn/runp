package cn.shiyanjun.running.platform;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Throwables;

import cn.shiyanjun.platform.api.LifecycleAware;
import cn.shiyanjun.platform.api.common.ContextImpl;
import cn.shiyanjun.platform.api.utils.NamedThreadFactory;
import cn.shiyanjun.platform.network.NettyRpcClient;
import cn.shiyanjun.platform.network.api.MessageDispatcher;
import cn.shiyanjun.platform.network.common.RpcService;
import cn.shiyanjun.running.platform.worker.ClientConnectionManager;
import cn.shiyanjun.running.platform.worker.WorkerChannelHandler;
import cn.shiyanjun.running.platform.worker.WorkerContext;
import cn.shiyanjun.running.platform.worker.WorkerMessageDispatcher;
import cn.shiyanjun.running.platform.worker.WorkerRpcService;

public class Worker implements LifecycleAware {

	private static final Log LOG = LogFactory.getLog(Worker.class);
	private final WorkerContext workerContext;
	private final MessageDispatcher dispatcher;
	private ExecutorService executorService;
	private final RpcService rpcService;
	private final ClientConnectionManager clientConnectionManager;
	
	public Worker(WorkerContext wContext) {
		this.workerContext = wContext;
		dispatcher = new WorkerMessageDispatcher(workerContext);
		
		clientConnectionManager = new ClientConnectionManager(workerContext, workerContext.getMessageidGenerator(), dispatcher);
		workerContext.setClientConnectionManager(clientConnectionManager);
		workerContext.setMessageDispatcher(dispatcher);
		rpcService = new WorkerRpcService(workerContext);
		workerContext.setRpcService(rpcService);
	}

	@Override
	public void start() {
		try {
			rpcService.start();
			executorService = Executors.newCachedThreadPool(new NamedThreadFactory("WORKER"));
			clientConnectionManager.startEndpoint(NettyRpcClient.class, WorkerChannelHandler.class);
			LOG.info("Worker started.");
			
			dispatcher.start();
			clientConnectionManager.getEndpoint().await();
		} catch (Exception e) {
			Throwables.propagate(e);
		}
	}
	
	@Override
	public void stop() {
		clientConnectionManager.getEndpoint().stop();
		executorService.shutdown();
	}
	
	public static void main(String[] args) {
		final WorkerContext workerContext = new WorkerContext(new ContextImpl());
		Worker worker = new Worker(workerContext);
		worker.start();		
	}

}
