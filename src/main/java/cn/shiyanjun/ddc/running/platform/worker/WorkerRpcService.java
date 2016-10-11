package cn.shiyanjun.ddc.running.platform.worker;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Throwables;

import cn.shiyanjun.ddc.network.NettyRpcClient;
import cn.shiyanjun.ddc.network.common.InboxMessage;
import cn.shiyanjun.ddc.network.common.OutboxMessage;
import cn.shiyanjun.ddc.network.common.PeerMessage;
import cn.shiyanjun.ddc.network.common.RpcMessage;
import cn.shiyanjun.ddc.network.common.RpcService;
import io.netty.channel.Channel;

public class WorkerRpcService extends RpcService {

	private static final Log LOG = LogFactory.getLog(WorkerRpcService.class);
	private volatile Channel masterChannel;
	private final WorkerContext workerContext;
	private final ClientConnectionManager clientConnectionManager;
	
	public WorkerRpcService(WorkerContext workerContext) {
		super(workerContext.getContext(), workerContext.getMessageDispatcher());
		this.workerContext = workerContext;
		clientConnectionManager = workerContext.getClientConnectionManager();
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
			workerContext.update(workerContext.getMasterId(), masterChannel);
		} else {
			assert channel != null;
			assert message != null;
			
			if(masterChannel == null || !masterChannel.isActive()) {
				masterChannel = channel;
				workerContext.update(workerContext.getMasterId(), masterChannel);
			}
			// route message to inbox
			InboxMessage inboxMessage = new InboxMessage();
			inboxMessage.setRpcMessage(message);
			inboxMessage.setFromEndpointId(workerContext.getPeerId(channel));
			inboxMessage.setToEndpointId(workerContext.getPeerId());
			inboxMessage.setChannel(channel);
			inbox.collect(inboxMessage);
			LOG.debug("Inbox collected: rpcMessage=" + message);
		}
	}

	@Override
	public void receive(Channel channel, Throwable cause) {
		if(masterChannel != null) {
			workerContext.remove(workerContext.getMasterId(), masterChannel);
			masterChannel = null;
		}
		
		while(true) {
			try {
				clientConnectionManager.stopEndpoint();
				
				LOG.info("Try to connect to master...");
				clientConnectionManager.startEndpoint(NettyRpcClient.class, WorkerChannelHandler.class);
				LOG.info("Connected.");
				
				// re-register to master
				LOG.info("Try to re-register to master...");
				clientConnectionManager.registerToMaster();
				break;
			} catch (Exception e) {
				try {
					Thread.sleep(workerContext.getRpcRetryConnectIntervalMillis());
				} catch (InterruptedException e1) { }
			}
		}
		
		// after endpoint started, await
		try {
			clientConnectionManager.getEndpoint().await();
		} catch (InterruptedException e) {
			Throwables.propagate(e);
		}
	}
	
	

}
