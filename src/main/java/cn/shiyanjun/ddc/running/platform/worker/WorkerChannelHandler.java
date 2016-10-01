package cn.shiyanjun.ddc.running.platform.worker;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.shiyanjun.ddc.api.Context;
import cn.shiyanjun.ddc.network.common.MessageDispatcher;
import cn.shiyanjun.ddc.running.platform.common.AbstractChannelHandler;
import io.netty.channel.ChannelHandlerContext;

public class WorkerChannelHandler extends AbstractChannelHandler {

	private static final Log LOG = LogFactory.getLog(WorkerChannelHandler.class);
	
	public WorkerChannelHandler(Context context, MessageDispatcher dispatcher) {
		super(context, dispatcher);
	}
	
	@Override
	public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
		super.channelRegistered(ctx);
		LOG.info("Master channel registered: channel=" + ctx.channel());
	}
	
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		super.channelRead(ctx, msg);
	}

}
