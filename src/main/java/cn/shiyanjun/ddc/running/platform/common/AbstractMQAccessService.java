package cn.shiyanjun.ddc.running.platform.common;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Throwables;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import cn.shiyanjun.ddc.running.platform.api.MQAccessService;

public abstract class AbstractMQAccessService implements MQAccessService {

	private static final Log LOG = LogFactory.getLog(AbstractMQAccessService.class);
	private final ConnectionFactory connectionFactory;
	protected Channel channel;
	protected Connection connection;
	protected final String queueName;
	 
	public AbstractMQAccessService(String queueName, ConnectionFactory connectionFactory) {
		super();
		this.queueName = queueName;
		this.connectionFactory = connectionFactory;
	}
	
	@Override
	public void start() {
		try {
			connection = connectionFactory.newConnection();	
			channel = connection.createChannel();
			Map<String, Object> args = new HashMap<String, Object>();
			channel.queueDeclare(queueName, true, false, false, args);
		} catch (Exception e) {
			LOG.error("Fail to establish Rabbitmq connection.");
			Throwables.propagate(e);
		}
	}
	
	@Override
	public void stop() {
		try {
			channel.close();
			connection.close();
		} catch (Exception e) {
			LOG.warn("Failed to close: connection=" + connection + ", channel=" + channel);
		}
	}
	
	@Override
	public Channel getChannel() {
		return channel;
	}
	
	@Override
	public String getQueueName() {
		return queueName;
	}
	
}
