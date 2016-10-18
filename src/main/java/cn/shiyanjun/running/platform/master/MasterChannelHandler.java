package cn.shiyanjun.running.platform.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.shiyanjun.running.platform.common.AbstractChannelHandler;
import io.netty.channel.ChannelHandlerContext;

public class MasterChannelHandler extends AbstractChannelHandler {

	private static final Log LOG = LogFactory.getLog(MasterChannelHandler.class);
	
	public MasterChannelHandler(MasterContext context) {
		super(context);
	}
	
	@Override
	public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
		super.channelRegistered(ctx);
		LOG.info("Worker channel registered: channel=" + ctx.channel());
	}
	
}
