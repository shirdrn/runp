package cn.shiyanjun.ddc.running.platform.master;

import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;

import cn.shiyanjun.ddc.api.Context;
import cn.shiyanjun.ddc.network.common.InboxMessage;
import cn.shiyanjun.ddc.network.common.LocalMessage;
import cn.shiyanjun.ddc.network.common.MessageDispatcher;
import cn.shiyanjun.ddc.network.common.OutboxMessage;
import cn.shiyanjun.ddc.network.common.RpcMessage;
import cn.shiyanjun.ddc.network.common.RpcMessageHandler;
import cn.shiyanjun.ddc.running.platform.constants.JsonKeys;
import cn.shiyanjun.ddc.running.platform.constants.MessageType;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

public class MasterRpcMessageHandler extends RpcMessageHandler {

	private static final Log LOG = LogFactory.getLog(MasterRpcMessageHandler.class);
	private final ConcurrentMap<String, Channel> workerIdToChannel = Maps.newConcurrentMap();
	private final ConcurrentMap<Channel, String> channelToWorkerId = Maps.newConcurrentMap();
	
	public MasterRpcMessageHandler(Context context, MessageDispatcher dispatcher) {
		super(context, dispatcher);
	}

	@Override
	protected void sendToRemotePeer(LocalMessage request, boolean needRelpy, int timeoutMillis) {
		RpcMessage m = request.getRpcMessage();
		String to = request.getToEndpointId();
		Channel channel = workerIdToChannel.get(to);
		LOG.debug("Send to worker: workerChannel=" + channel);
		LOG.debug("Send to worker: rpcMessage=" + m);
		
		OutboxMessage message = new OutboxMessage();
		message.setRpcMessage(m);
		message.setChannel(channel);
		message.setTimeoutMillis(timeoutMillis);
		outbox.collect(message);
	}
	
	@Override
	public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
		super.channelRegistered(ctx);
	}
	
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		super.channelRead(ctx, msg);
		RpcMessage m = (RpcMessage) msg;
		LOG.debug("Master channel read: rpcMessage=" + m);
		if(m.getType() == MessageType.WORKER_REGISTRATION.getCode()) {
			JSONObject body = JSONObject.parseObject(m.getBody());
			String workerId = body.getString(JsonKeys.WORKER_ID);
			Channel channel = workerIdToChannel.get(workerId);
			if(channel == null) {
				channel = ctx.channel();
				workerIdToChannel.putIfAbsent(workerId, channel);
				channelToWorkerId.putIfAbsent(channel, workerId);
			} else {
				if(!channel.isActive()) {
					workerIdToChannel.put(workerId, channel);
					channelToWorkerId.putIfAbsent(channel, workerId);
				}
			}
		}
		
		// route message to inbox
		InboxMessage message = new InboxMessage();
		String from = channelToWorkerId.get(ctx.channel());
		LOG.debug("Master channel read: workerId=" + from + ", channel=" + ctx.channel());
		message.setRpcMessage(m);
		message.setFromEndpointId(from);
		message.setToEndpointId(null);
		message.setChannel(ctx.channel());
		inbox.collect(message);
	}

}
