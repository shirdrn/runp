package cn.shiyanjun.ddc.running.platform.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONObject;

import cn.shiyanjun.ddc.network.common.InboxMessage;
import cn.shiyanjun.ddc.network.common.PeerMessage;
import cn.shiyanjun.ddc.network.common.OutboxMessage;
import cn.shiyanjun.ddc.network.common.RpcMessage;
import cn.shiyanjun.ddc.network.common.RpcService;
import cn.shiyanjun.ddc.running.platform.common.RunpContext;
import cn.shiyanjun.ddc.running.platform.constants.JsonKeys;
import cn.shiyanjun.ddc.running.platform.constants.MessageType;
import io.netty.channel.Channel;

public class MasterRpcService extends RpcService {

	private static final Log LOG = LogFactory.getLog(MasterRpcService.class);
	private final RunpContext context;
	
	public MasterRpcService(RunpContext context) {
		super(context.getContext(), context.getMessageDispatcher());
		this.context = context;
	}
	
	@Override
	protected void sendToRemotePeer(PeerMessage request, boolean needRelpy, int timeoutMillis) {
		RpcMessage m = request.getRpcMessage();
		String to = request.getToEndpointId();
		Channel channel = context.getChannel(to);
		LOG.debug("Send to worker: workerChannel=" + channel);
		LOG.debug("Send to worker: rpcMessage=" + m);
		
		OutboxMessage message = new OutboxMessage();
		message.setRpcMessage(m);
		message.setChannel(channel);
		message.setTimeoutMillis(timeoutMillis);
		outbox.collect(message);
	}

	@Override
	public void receive(Channel channel, RpcMessage mmessage) {
		LOG.debug("Master channel read: rpcMessage=" + mmessage);
		if(mmessage != null) {
			if(mmessage.getType() == MessageType.WORKER_REGISTRATION.getCode()) {
				JSONObject body = JSONObject.parseObject(mmessage.getBody());
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
			inboxMessage.setRpcMessage(mmessage);
			inboxMessage.setFromEndpointId(from);
			inboxMessage.setToEndpointId(context.getThisPeerId());
			inboxMessage.setChannel(channel);
			inbox.collect(inboxMessage);		
		}
	}

}
