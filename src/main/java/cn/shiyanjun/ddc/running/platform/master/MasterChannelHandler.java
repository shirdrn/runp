package cn.shiyanjun.ddc.running.platform.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.shiyanjun.ddc.api.Context;
import cn.shiyanjun.ddc.network.common.MessageDispatcher;
import cn.shiyanjun.ddc.running.platform.common.AbstractChannelHandler;
import io.netty.channel.ChannelHandlerContext;

public class MasterChannelHandler extends AbstractChannelHandler {

	private static final Log LOG = LogFactory.getLog(MasterChannelHandler.class);
	
	public MasterChannelHandler(Context context, MessageDispatcher messageDispatcher) {
		super(context, messageDispatcher);
	}

	@Override
	public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
		super.channelRegistered(ctx);
		LOG.info("Worker channel registered: channel=" + ctx.channel());
	}
	
}
