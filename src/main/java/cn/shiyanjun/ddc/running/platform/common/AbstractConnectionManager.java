package cn.shiyanjun.ddc.running.platform.common;

import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import cn.shiyanjun.ddc.api.Context;
import cn.shiyanjun.ddc.api.common.AbstractComponent;
import cn.shiyanjun.ddc.api.utils.Pair;
import cn.shiyanjun.ddc.network.common.MessageDispatcher;
import cn.shiyanjun.ddc.network.common.NettyRpcEndpoint;
import cn.shiyanjun.ddc.running.platform.worker.WorkerChannelHandler;
import io.netty.channel.ChannelHandler;

public abstract class AbstractConnectionManager extends AbstractComponent implements ConnectionManager {

	protected MessageDispatcher messageDispatcher;
	protected volatile NettyRpcEndpoint endpoint;
	
	public AbstractConnectionManager(Context context) {
		super(context);
	}
	
	@Override
	public void setMessageDispatcher(MessageDispatcher messageDispatcher) {
		this.messageDispatcher = messageDispatcher;		
	}
	
	@Override
	public void startEndpoint(Class<? extends NettyRpcEndpoint> endpointClass) throws Exception {
		Preconditions.checkArgument(messageDispatcher != null, "Message dispatcher not set!");
		endpoint = createRpcEndpoint(endpointClass);
		endpoint.start();
	}
	
	private NettyRpcEndpoint createRpcEndpoint(Class<? extends NettyRpcEndpoint> endpointClass) {
		List<Pair<Class<? extends ChannelHandler>, Object[]>> handlerInfos = Lists.newArrayList();
		handlerInfos.add(new Pair<Class<? extends ChannelHandler>, Object[]>(
				WorkerChannelHandler.class, 
				new Object[] { context, messageDispatcher }));
		return NettyRpcEndpoint.newEndpoint(
				context, 
				endpointClass, 
				handlerInfos);
	}

	public NettyRpcEndpoint getEndpoint() {
		return endpoint;
	}

}
