package cn.shiyanjun.ddc.running.platform.common;

import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.Maps;

import cn.shiyanjun.ddc.api.Context;
import cn.shiyanjun.ddc.network.common.MessageDispatcher;
import cn.shiyanjun.ddc.network.common.RpcService;
import io.netty.channel.Channel;

public class RunpContext {

	protected String masterId = "Master";
	protected String thisPeerId;
	protected final ConcurrentMap<String, Channel> peerIdToChannel = Maps.newConcurrentMap();
	protected final ConcurrentMap<Channel, String> channelToPeerId = Maps.newConcurrentMap();
	protected Context  context;
	protected MessageDispatcher messageDispatcher;
	protected RpcService rpcService;
	
	public Channel getChannel(String peerId) {
		return peerIdToChannel.get(peerId);
	}
	
	public String getPeerId(Channel channel) {
		return channelToPeerId.get(channel);
	}
	
	public void remove(String peerId, Channel channel) {
		peerIdToChannel.remove(peerId, channel);
		channelToPeerId.remove(channel, peerId);
	}
	
	public void update(String peerId, Channel channel) {
		peerIdToChannel.put(peerId, channel);
		channelToPeerId.put(channel, peerId);
	}

	public Context getContext() {
		return context;
	}

	public void setContext(Context context) {
		this.context = context;
	}

	public MessageDispatcher getMessageDispatcher() {
		return messageDispatcher;
	}

	public void setMessageDispatcher(MessageDispatcher messageDispatcher) {
		this.messageDispatcher = messageDispatcher;
	}

	public RpcService getRpcService() {
		return rpcService;
	}

	public void setRpcService(RpcService rpcService) {
		this.rpcService = rpcService;
	}

	public String getThisPeerId() {
		return thisPeerId;
	}

	public void setThisPeerId(String thisPeerId) {
		this.thisPeerId = thisPeerId;
	}

	public String getMasterId() {
		return masterId;
	}

	public void setMasterId(String masterId) {
		this.masterId = masterId;
	}

	
}
