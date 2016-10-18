package cn.shiyanjun.running.platform.api;

import cn.shiyanjun.platform.network.common.NettyRpcEndpoint;
import io.netty.channel.ChannelHandler;

public interface ConnectionManager {

	void startEndpoint(Class<? extends NettyRpcEndpoint> endpointClass, Class<? extends ChannelHandler> handlerClass) throws Exception;
	NettyRpcEndpoint getEndpoint();
	
	void stopEndpoint();
}
