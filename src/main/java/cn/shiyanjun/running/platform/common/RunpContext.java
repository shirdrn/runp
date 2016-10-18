package cn.shiyanjun.running.platform.common;

import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.Maps;

import cn.shiyanjun.platform.api.Context;
import cn.shiyanjun.platform.network.api.MessageDispatcher;
import cn.shiyanjun.platform.network.common.RpcService;
import cn.shiyanjun.platform.network.constants.RpcConfigKeys;
import cn.shiyanjun.running.platform.constants.RunpConfigKeys;
import io.netty.channel.Channel;

public abstract class RunpContext {

	private final String masterId;
	protected String peerId;
	protected final ConcurrentMap<String, Channel> peerIdToChannel = Maps.newConcurrentMap();
	protected final ConcurrentMap<Channel, String> channelToPeerId = Maps.newConcurrentMap();
	protected final  Context  context;
	protected MessageDispatcher messageDispatcher;
	protected RpcService rpcService;
	protected final int rpcRetryConnectIntervalMillis;
	
	public RunpContext(Context context) {
		super();
		this.context = context;
		masterId = context.get(RunpConfigKeys.MASTER_ID, "Master");
		rpcRetryConnectIntervalMillis = context.getInt(RpcConfigKeys.NETWORK_RPC_RETRY_CONNECT_INTERVALMILLIS, 10000);
	}
	
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

	public String getPeerId() {
		return peerId;
	}

	public String getMasterId() {
		return masterId;
	}

	public Context getContext() {
		return context;
	}

	public int getRpcRetryConnectIntervalMillis() {
		return rpcRetryConnectIntervalMillis;
	}

}
