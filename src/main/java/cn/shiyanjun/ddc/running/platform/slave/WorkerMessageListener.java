package cn.shiyanjun.ddc.running.platform.slave;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.shiyanjun.ddc.api.Context;
import cn.shiyanjun.ddc.api.constants.ConfigKeys;
import cn.shiyanjun.ddc.api.network.MessageListener;
import cn.shiyanjun.ddc.api.utils.NamedThreadFactory;
import cn.shiyanjun.ddc.network.common.AbstractMessageListener;
import cn.shiyanjun.ddc.network.common.RpcChannelHandler;
import cn.shiyanjun.ddc.network.common.RpcMessage;
import cn.shiyanjun.ddc.running.platform.constants.MessageTypes;
/**
 * In <code>Worker</code> side, its responsibility is to handle messages 
 * from <code>Master</code> side, such as consuming received task command, 
 * and in current side, such as sending messages to <code>Master</code>.
 * 
 * @author yanjun
 */

public class WorkerMessageListener extends AbstractMessageListener implements MessageListener<RpcMessage> {

	private static final Log LOG = LogFactory.getLog(WorkerMessageListener.class);
	private ScheduledExecutorService scheduledExecutorService;
	private final AtomicLong idGenerator = new AtomicLong();
	
	public WorkerMessageListener(Context context, RpcChannelHandler rpcChannelHandler) {
		super(context, rpcChannelHandler);
		scheduledExecutorService = Executors.newScheduledThreadPool(1, new NamedThreadFactory("HEARTBEAT"));
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				scheduledExecutorService.shutdown();
				LOG.info("Scheduled executor shutdown: " + scheduledExecutorService);
			}
		});
		
		int period = context.getInt(ConfigKeys.RPC_HEARTBEAT_INTERVAL, 5000);
		scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				Long id = idGenerator.incrementAndGet();
				RpcMessage hb = new RpcMessage(id, MessageTypes.HEART_BEAT);
				hb.setBody("{\"ip\":\"10.10.0.123\"}");
				WorkerMessageListener.this.getRpcChannelHandler().ask(hb);				
			}
			
		}, 1, period, TimeUnit.MILLISECONDS);
	}
	
	@Override
	public void handle(RpcMessage message) {
		int type = message.getType();
		switch(type) {
			case MessageTypes.TASK_ASSIGNMENT:
				LOG.info("client.recv.task.assignment====" + message);
				break;
			default:
				
		}
		
	}

}
