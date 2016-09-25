package cn.shiyanjun.ddc.running.platform.common;

import io.netty.channel.Channel;

public class WorkerInfo {

	private String host;
	private Channel channel;
	private long lastContatTime;
	
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public Channel getChannel() {
		return channel;
	}
	public void setChannel(Channel channel) {
		this.channel = channel;
	}
	public long getLastContatTime() {
		return lastContatTime;
	}
	public void setLastContatTime(long lastContatTime) {
		this.lastContatTime = lastContatTime;
	}

}
