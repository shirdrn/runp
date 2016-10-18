package cn.shiyanjun.running.platform.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.shiyanjun.platform.network.common.RpcMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public abstract class AbstractChannelHandler extends ChannelInboundHandlerAdapter {

	private static final Log LOG = LogFactory.getLog(AbstractChannelHandler.class);
	protected final RunpContext context;
	
	public AbstractChannelHandler(RunpContext context) {
		super();
		this.context = context;
	}
	
	@Override
	public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
		super.channelUnregistered(ctx);
		ctx.channel().close();
		context.getRpcService().receive(ctx.channel(), new Exception("Channel unregisted"));
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		super.exceptionCaught(ctx, cause);
		ctx.channel().close();
		context.getRpcService().receive(ctx.channel(), cause);
	}
	
	@Override
	public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
		super.channelRegistered(ctx);
		context.getRpcService().receive(ctx.channel(), (RpcMessage) null);
	}
	
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		super.channelRead(ctx, msg);
		RpcMessage message = (RpcMessage) msg;
		LOG.debug("Channel read: rpcMessage=" + message);
		context.getRpcService().receive(ctx.channel(), message);
	}
}
