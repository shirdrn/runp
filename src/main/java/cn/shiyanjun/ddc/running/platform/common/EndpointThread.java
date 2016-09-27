package cn.shiyanjun.ddc.running.platform.common;

import cn.shiyanjun.ddc.network.common.NettyRpcEndpoint;

public class EndpointThread implements Runnable {

	private NettyRpcEndpoint endpoint;
	
	@Override
	public void run() {
		endpoint.start();
	}

	public NettyRpcEndpoint getEndpoint() {
		return endpoint;
	}

	public void setEndpoint(NettyRpcEndpoint endpoint) {
		this.endpoint = endpoint;
	}

}
