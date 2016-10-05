package cn.shiyanjun.ddc.running.platform.common;

import cn.shiyanjun.ddc.network.common.MessageDispatcher;
import cn.shiyanjun.ddc.network.common.NettyRpcEndpoint;

public interface ConnectionManager {

	void setMessageDispatcher(MessageDispatcher messageDispatcher);
	void startEndpoint(Class<? extends NettyRpcEndpoint> endpointClass) throws Exception;
	void recoverState();
}
