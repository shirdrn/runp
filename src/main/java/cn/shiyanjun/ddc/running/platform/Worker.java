package cn.shiyanjun.ddc.running.platform;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import cn.shiyanjun.ddc.api.LifecycleAware;
import cn.shiyanjun.ddc.api.common.AbstractComponent;
import cn.shiyanjun.ddc.api.common.ContextImpl;
import cn.shiyanjun.ddc.api.utils.NamedThreadFactory;
import cn.shiyanjun.ddc.api.utils.Pair;
import cn.shiyanjun.ddc.network.NettyRpcClient;
import cn.shiyanjun.ddc.network.common.MessageDispatcher;
import cn.shiyanjun.ddc.network.common.NettyRpcEndpoint;
import cn.shiyanjun.ddc.network.common.RpcService;
import cn.shiyanjun.ddc.running.platform.common.RunpContext;
import cn.shiyanjun.ddc.running.platform.constants.RunpConfigKeys;
import cn.shiyanjun.ddc.running.platform.worker.WorkerChannelHandler;
import cn.shiyanjun.ddc.running.platform.worker.WorkerMessageDispatcher;
import cn.shiyanjun.ddc.running.platform.worker.WorkerRpcService;
import io.netty.channel.ChannelHandler;

public class Worker extends AbstractComponent implements LifecycleAware {

	private static final Log LOG = LogFactory.getLog(Worker.class);
	private final NettyRpcEndpoint endpoint;
	private final MessageDispatcher dispatcher;
	private ExecutorService executorService;
	private final String workerId;
	private final String workerHost;
	private final RpcService rpcService;
	
	public Worker(RunpContext context) {
		super(context.getContext());
		workerId = context.getContext().get(RunpConfigKeys.WORKER_ID);
		workerHost = context.getContext().get(RunpConfigKeys.WORKER_HOST);
		Preconditions.checkArgument(workerId != null);
		Preconditions.checkArgument(workerHost != null);
		context.setThisPeerId(workerId);
		
		dispatcher = new WorkerMessageDispatcher(context);
		context.setMessageDispatcher(dispatcher);
		rpcService = new WorkerRpcService(context);
		dispatcher.setRpcService(rpcService);
		context.setRpcService(rpcService);
		
		
		List<Pair<Class<? extends ChannelHandler>, Object[]>> handlerInfos = Lists.newArrayList();
		handlerInfos.add(new Pair<Class<? extends ChannelHandler>, Object[]>(WorkerChannelHandler.class, new Object[] {context.getContext(), dispatcher}));
		endpoint = NettyRpcEndpoint.newEndpoint(
				context.getContext(), 
				NettyRpcClient.class, 
				handlerInfos);
	}
	
	@Override
	public void start() {
		try {
			rpcService.start();
			executorService = Executors.newCachedThreadPool(new NamedThreadFactory("WORKER"));
			executorService.execute(new Runnable() {

				@Override
				public void run() {
					endpoint.start();					
				}
				
			});
			Thread.sleep(1000);
			
			dispatcher.start();
			LOG.info("Worker started.");
		} catch (Exception e) {
			Throwables.propagate(e);
		}
	}
	
	@Override
	public void stop() {
		endpoint.stop();
		executorService.shutdown();
	}
	
	public static void main(String[] args) {
		final RunpContext context = new RunpContext();
		context.setContext(new ContextImpl());
		Worker worker = new Worker(context);
		worker.start();		
	}

}
