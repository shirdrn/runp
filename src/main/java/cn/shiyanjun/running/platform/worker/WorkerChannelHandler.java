package cn.shiyanjun.running.platform.worker;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.shiyanjun.running.platform.common.AbstractChannelHandler;
import io.netty.channel.ChannelHandlerContext;

public class WorkerChannelHandler extends AbstractChannelHandler {

	private static final Log LOG = LogFactory.getLog(WorkerChannelHandler.class);
	
	public WorkerChannelHandler(WorkerContext context) {
		super(context);
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
