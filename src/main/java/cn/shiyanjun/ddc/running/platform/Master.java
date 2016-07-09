package cn.shiyanjun.ddc.running.platform;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.shiyanjun.ddc.api.Context;
import cn.shiyanjun.ddc.api.LifecycleAware;
import cn.shiyanjun.ddc.api.common.AbstractComponent;
import cn.shiyanjun.ddc.api.common.ContextImpl;
import cn.shiyanjun.ddc.api.network.MessageListener;
import cn.shiyanjun.ddc.api.utils.NamedThreadFactory;
import cn.shiyanjun.ddc.network.NettyRpcClient;
import cn.shiyanjun.ddc.network.common.RpcChannelHandler;
import cn.shiyanjun.ddc.network.common.RpcMessage;
import cn.shiyanjun.ddc.network.common.RpcMessageHandler;
import cn.shiyanjun.ddc.running.platform.common.MqMessageAccessor;
import cn.shiyanjun.ddc.running.platform.common.TaskAssignmentProtocol;
import cn.shiyanjun.ddc.running.platform.master.MasterMessageListener;

/**
 * Master is the coordinator of running platform, its responsibility is
 * to accept {@link Worker}s' heartbeat messages to acquire the states and resources
 *  of each worker node. And finally the Scheduling Platform should be told to
 *  decide next scheduling choice.
 * 
 * @author yanjun
 */
public class Master extends AbstractComponent implements LifecycleAware {

	private static final Log LOG = LogFactory.getLog(Master.class);
	private LifecycleAware endpoint;
	private final RpcChannelHandler rpcHandler;
	private final MessageListener<RpcMessage> messageListener;
	private final TaskAssignmentProtocol taskAssignment;
	private ExecutorService executorService;
	private final MqMessageAccessor mqMessageAccessor;
	private final String taskQueueName = "taskq";
	
	public Master(Context context) {
		super(context);
		rpcHandler = new RpcMessageHandler(context);
		messageListener = new MasterMessageListener(context, rpcHandler);
		rpcHandler.setMessageListener(messageListener);
		taskAssignment = (TaskAssignmentProtocol) messageListener;
		mqMessageAccessor = null;
	}
	
	@Override
	public void start() {
		endpoint = NettyRpcClient.newClient(getContext(), rpcHandler, messageListener);
		executorService = Executors.newFixedThreadPool(1, new NamedThreadFactory("MASTER"));
		executorService.execute(new PullTaskThread());
		endpoint.start();		
	}

	@Override
	public void stop() {
		endpoint.stop();		
	}
	
	private final class PullTaskThread implements Runnable {
		
		@Override
		public void run() {
			while(true) {
				try {
					String jsonTask = mqMessageAccessor.fetch(taskQueueName);
					if(jsonTask != null) {
						RpcMessage msg = new RpcMessage();
						msg.setBody(jsonTask);
						taskAssignment.assign(msg);
					}
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	public static void main(String[] args) {
		final Context context = new ContextImpl();
		Master master = new Master(context);
		master.start();		
	}

}
