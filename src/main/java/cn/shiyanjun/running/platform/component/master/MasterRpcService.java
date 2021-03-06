package cn.shiyanjun.running.platform.component.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONObject;

import cn.shiyanjun.platform.network.common.InboxMessage;
import cn.shiyanjun.platform.network.common.OutboxMessage;
import cn.shiyanjun.platform.network.common.PeerMessage;
import cn.shiyanjun.platform.network.common.RpcMessage;
import cn.shiyanjun.platform.network.common.RpcService;
import cn.shiyanjun.running.platform.constants.JsonKeys;
import cn.shiyanjun.running.platform.constants.MessageType;
import io.netty.channel.Channel;

public class MasterRpcService extends RpcService {

	private static final Log LOG = LogFactory.getLog(MasterRpcService.class);
	private final MasterContext context;
	
	public MasterRpcService(MasterContext context) {
		super(context.getContext(), context.getMessageDispatcher());
		this.context = context;
	}
	
	@Override
	protected void sendToRemotePeer(PeerMessage request, boolean needRelpy, int timeoutMillis) {
		RpcMessage rpcMessage = request.getRpcMessage();
		String to = request.getToEndpointId();
		Channel channel = context.getChannel(to);
		LOG.debug("Send to worker: workerChannel=" + channel);
		LOG.debug("Send to worker: rpcMessage=" + rpcMessage);
		
		OutboxMessage outboxMessage = new OutboxMessage();
		outboxMessage.setRpcMessage(rpcMessage);
		outboxMessage.setChannel(channel);
		outboxMessage.setTimeoutMillis(timeoutMillis);
		outbox.collect(outboxMessage);
	}

	@Override
	public void receive(Channel channel, RpcMessage rpcMessage) {
		LOG.debug("Master channel read: rpcMessage=" + rpcMessage);
		if(rpcMessage != null) {
			if(rpcMessage.getType() == MessageType.WORKER_REGISTRATION.getCode()) {
				JSONObject body = JSONObject.parseObject(rpcMessage.getBody());
				String workerId = body.getString(JsonKeys.WORKER_ID);
				Channel ch = context.getChannel(workerId);
				if(ch == null) {
					ch = channel;
					context.update(workerId, ch);
				} else {
					if(!ch.isActive()) {
						context.update(workerId, ch);
					}
				}
			}
			
			// route message to inbox
			InboxMessage inboxMessage = new InboxMessage();
			String from = context.getPeerId(channel);
			LOG.debug("Master channel read: workerId=" + from + ", channel=" + channel);
			
			inboxMessage.setRpcMessage(rpcMessage);
			inboxMessage.setFromEndpointId(from);
			inboxMessage.setToEndpointId(context.getPeerId());
			inboxMessage.setChannel(channel);
			inbox.collect(inboxMessage);		
		}
	}

	@Override
	public void receive(Channel channel, Throwable cause) {
		String workerId = context.getPeerId(channel);
		context.remove(workerId, channel);	
		context.removeResource(workerId);
		LOG.warn("Channel closed: " + channel, cause);
		LOG.info("Purge registered worker with resources: workerId=" + workerId + ", channel=" + channel);
	}

}
