package cn.shiyanjun.ddc.running.platform;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import cn.shiyanjun.ddc.api.LifecycleAware;
import cn.shiyanjun.ddc.api.common.AbstractComponent;
import cn.shiyanjun.ddc.api.common.ContextImpl;
import cn.shiyanjun.ddc.api.utils.NamedThreadFactory;
import cn.shiyanjun.ddc.network.NettyRpcClient;
import cn.shiyanjun.ddc.network.common.MessageDispatcher;
import cn.shiyanjun.ddc.network.common.RpcService;
import cn.shiyanjun.ddc.running.platform.common.WorkerContext;
import cn.shiyanjun.ddc.running.platform.constants.RunpConfigKeys;
import cn.shiyanjun.ddc.running.platform.worker.ClientConnectionManager;
import cn.shiyanjun.ddc.running.platform.worker.WorkerMessageDispatcher;
import cn.shiyanjun.ddc.running.platform.worker.WorkerRpcService;

public class Worker extends AbstractComponent implements LifecycleAware {

	private static final Log LOG = LogFactory.getLog(Worker.class);
	private final WorkerContext workerContext;
	private final MessageDispatcher dispatcher;
	private ExecutorService executorService;
	private final String workerId;
	private final String workerHost;
	private final RpcService rpcService;
	private final ClientConnectionManager clientConnectionManager;
	
	public Worker(WorkerContext wContext) {
		super(wContext.getContext());
		this.workerContext = wContext;
		workerId = workerContext.getContext().get(RunpConfigKeys.WORKER_ID);
		workerHost = workerContext.getContext().get(RunpConfigKeys.WORKER_HOST);
		Preconditions.checkArgument(workerId != null);
		Preconditions.checkArgument(workerHost != null);
		workerContext.setThisPeerId(workerId);
		
		dispatcher = new WorkerMessageDispatcher(workerContext);
		
		clientConnectionManager = new ClientConnectionManager(workerContext, workerContext.getMessageidGenerator(), dispatcher);
		workerContext.setClientConnectionManager(clientConnectionManager);
		workerContext.setMessageDispatcher(dispatcher);
		rpcService = new WorkerRpcService(workerContext);
		dispatcher.setRpcService(rpcService);
		workerContext.setRpcService(rpcService);
	}

	@Override
	public void start() {
		try {
			rpcService.start();
			executorService = Executors.newCachedThreadPool(new NamedThreadFactory("WORKER"));
			clientConnectionManager.startEndpoint(NettyRpcClient.class);
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
