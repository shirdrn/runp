package cn.shiyanjun.ddc.running.platform.common;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.Lists;

import cn.shiyanjun.ddc.api.common.AbstractComponent;
import cn.shiyanjun.ddc.api.utils.Pair;
import cn.shiyanjun.ddc.network.common.NettyRpcEndpoint;
import cn.shiyanjun.ddc.running.platform.api.ConnectionManager;
import io.netty.channel.ChannelHandler;

public abstract class AbstractConnectionManager extends AbstractComponent implements ConnectionManager {

	private static final Log LOG = LogFactory.getLog(AbstractConnectionManager.class);
	protected volatile NettyRpcEndpoint endpoint;
	private final RunpContext context;
	
	public AbstractConnectionManager(RunpContext context) {
		super(context.getContext());
		this.context = context;
	}
	
	@Override
	public void startEndpoint(
			Class<? extends NettyRpcEndpoint> endpointClass, 
			Class<? extends ChannelHandler> handlerClass) throws Exception {
		List<Pair<Class<? extends ChannelHandler>, Object[]>> handlerInfos = Lists.newArrayList();
		handlerInfos.add(new Pair<Class<? extends ChannelHandler>, Object[]>(
				handlerClass, 
				new Object[] { context }));
		endpoint = NettyRpcEndpoint.newEndpoint(context.getContext(), endpointClass, handlerInfos);
		endpoint.start();
	}
	
	@Override
	public void stopEndpoint() {
		if(endpoint != null) {
			endpoint.stop();
			LOG.info("Endpoint stopped: " + endpoint);
		}
	}
	
	@Override
	public NettyRpcEndpoint getEndpoint() {
		return endpoint;
	}

}
