package cn.shiyanjun.ddc.running.platform.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.shiyanjun.ddc.api.Context;
import cn.shiyanjun.ddc.network.common.MessageDispatcher;
import cn.shiyanjun.ddc.network.common.RpcMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public abstract class AbstractChannelHandler extends ChannelInboundHandlerAdapter {

	private static final Log LOG = LogFactory.getLog(AbstractChannelHandler.class);
	protected final Context context;
	protected final MessageDispatcher messageDispatcher;
	
	public AbstractChannelHandler(Context context, MessageDispatcher messageDispatcher) {
		super();
		this.context = context;
		this.messageDispatcher = messageDispatcher;
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		super.exceptionCaught(ctx, cause);
		ctx.channel().close();
		messageDispatcher.getRpcService().receive(ctx.channel(), cause);
	}
	
	@Override
	public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
		super.channelRegistered(ctx);
		messageDispatcher.getRpcService().receive(ctx.channel(), (RpcMessage) null);
	}
	
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		super.channelRead(ctx, msg);
		RpcMessage message = (RpcMessage) msg;
		LOG.debug("Channel read: rpcMessage=" + message);
		messageDispatcher.getRpcService().receive(ctx.channel(), message);
	}
}
