package cn.shiyanjun.ddc.running.platform.common;

import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.Maps;

import cn.shiyanjun.ddc.api.Context;
import cn.shiyanjun.ddc.network.common.MessageDispatcher;
import cn.shiyanjun.ddc.network.common.RpcService;
import cn.shiyanjun.ddc.running.platform.api.TaskScheduler;
import io.netty.channel.Channel;

public class RunpContext {

	private String masterId = "Master";
	private String thisPeerId;
	private final ConcurrentMap<String, Channel> peerIdToChannel = Maps.newConcurrentMap();
	private final ConcurrentMap<Channel, String> channelToPeerId = Maps.newConcurrentMap();
	private Context  context;
	private MessageDispatcher messageDispatcher;
	private RpcService rpcService;
	private TaskScheduler taskScheduler;
	
	public Channel getChannel(String peerId) {
		return peerIdToChannel.get(peerId);
	}
	
	public String getPeerId(Channel channel) {
		return channelToPeerId.get(channel);
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

	public TaskScheduler getTaskScheduler() {
		return taskScheduler;
	}

	public void setTaskScheduler(TaskScheduler taskScheduler) {
		this.taskScheduler = taskScheduler;
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
