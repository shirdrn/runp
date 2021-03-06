package cn.shiyanjun.running.platform.common;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;

import cn.shiyanjun.running.platform.api.RunnableConsumer;

public abstract class AbstractRunnableConsumer extends DefaultConsumer implements RunnableConsumer {

	private static final Log LOG = LogFactory.getLog(AbstractRunnableConsumer.class);
	private final String queueName;
	
	public AbstractRunnableConsumer(String queueName, Channel channel) {
		super(channel);
		this.queueName = queueName;
	}

	@Override
	public void run() {
		try {
			String consumerTag = getChannel().basicConsume(queueName, false, this);
			LOG.info("Consumer started: consumerTag=" + consumerTag);
		} catch (IOException e) {
			throw new RuntimeException("Failed to start consumer: " ,e);
		}
	}
	
	@Override
	public int hashCode() {
		return queueName.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		AbstractRunnableConsumer other = (AbstractRunnableConsumer) obj;
		return this.hashCode() == other.hashCode();
	}

}
