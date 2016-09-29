package cn.shiyanjun.ddc.running.platform.worker;

import cn.shiyanjun.ddc.api.Context;
import cn.shiyanjun.ddc.network.common.InboxMessage;
import cn.shiyanjun.ddc.network.common.LocalMessage;
import cn.shiyanjun.ddc.network.common.MessageDispatcher;
import cn.shiyanjun.ddc.network.common.OutboxMessage;
import cn.shiyanjun.ddc.network.common.RpcMessage;
import cn.shiyanjun.ddc.network.common.RpcMessageHandler;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

public class WorkerRpcMessageHandler extends RpcMessageHandler {

	private volatile Channel masterChannel;
	
	public WorkerRpcMessageHandler(Context context, MessageDispatcher dispatcher) {
		super(context, dispatcher);
	}
	
	@Override
	public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
		super.channelRegistered(ctx);
		if(masterChannel == null) {
			masterChannel = ctx.channel();
		}
	}

	@Override
	protected void sendToRemotePeer(LocalMessage request, boolean needRelpy, int timeoutMillis) {
		OutboxMessage message = new OutboxMessage();
		message.setRpcMessage(request.getRpcMessage());
		message.getRpcMessage().setNeedReply(needRelpy);
		message.setChannel(masterChannel);
		message.setTimeoutMillis(timeoutMillis);
		outbox.collect(message);
	}
	
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		super.channelRead(ctx, msg);
		RpcMessage m = (RpcMessage) msg;
		
		// route message to inbox
		InboxMessage message = new InboxMessage();
		message.setRpcMessage(m);
		message.setFromEndpointId(null);
		message.setToEndpointId(null);
		message.setChannel(ctx.channel());
		inbox.collect(message);
	}

}
