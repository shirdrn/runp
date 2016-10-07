package cn.shiyanjun.ddc.running.platform.api;

import cn.shiyanjun.ddc.network.common.NettyRpcEndpoint;
import io.netty.channel.ChannelHandler;

public interface ConnectionManager {

	void startEndpoint(Class<? extends NettyRpcEndpoint> endpointClass, Class<? extends ChannelHandler> handlerClass) throws Exception;
	NettyRpcEndpoint getEndpoint();
}
