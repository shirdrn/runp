package cn.shiyanjun.ddc.running.platform.worker;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.shiyanjun.ddc.network.common.InboxMessage;
import cn.shiyanjun.ddc.network.common.PeerMessage;
import cn.shiyanjun.ddc.network.common.OutboxMessage;
import cn.shiyanjun.ddc.network.common.RpcMessage;
import cn.shiyanjun.ddc.network.common.RpcService;
import cn.shiyanjun.ddc.running.platform.common.RunpContext;
import io.netty.channel.Channel;

public class WorkerRpcService extends RpcService {

	private static final Log LOG = LogFactory.getLog(WorkerRpcService.class);
	private volatile Channel masterChannel;
	private final RunpContext context;
	
	public WorkerRpcService(RunpContext context) {
		super(context.getContext(), context.getMessageDispatcher());
		this.context = context;
	}

	@Override
	protected void sendToRemotePeer(PeerMessage request, boolean needRelpy, int timeoutMillis) {
		LOG.debug("Send to master: masterChannel=" + masterChannel);
		LOG.debug("Send to master: rpcMessage=" + request.getRpcMessage());
		RpcMessage rpcMessage = request.getRpcMessage();
		OutboxMessage outboxMessage = new OutboxMessage();
		outboxMessage.setRpcMessage(rpcMessage);
		outboxMessage.setChannel(masterChannel);
		outboxMessage.setTimeoutMillis(timeoutMillis);
		outbox.collect(outboxMessage);
		LOG.debug("Outbox collected: rpcMessage=" + rpcMessage);
	}
	
	@Override
	public void receive(Channel channel, RpcMessage message) {
		LOG.debug("Worker RPC service received: channel=" + channel + ", message=" + message);
		if(channel != null && message == null) {
			masterChannel = channel;
			context.update(context.getMasterId(), masterChannel);
		} else {
			assert channel != null;
			assert message != null;
			
			if(masterChannel == null || !masterChannel.isActive()) {
				masterChannel = channel;
				context.update(context.getMasterId(), masterChannel);
			}
			// route message to inbox
			InboxMessage inboxMessage = new InboxMessage();
			inboxMessage.setRpcMessage(message);
			inboxMessage.setFromEndpointId(context.getPeerId(channel));
			inboxMessage.setToEndpointId(context.getThisPeerId());
			inboxMessage.setChannel(channel);
			inbox.collect(inboxMessage);
			LOG.debug("Inbox collected: rpcMessage=" + message);
		}
	}
	
	

}
