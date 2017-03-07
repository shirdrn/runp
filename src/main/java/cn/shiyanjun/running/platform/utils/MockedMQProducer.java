package cn.shiyanjun.running.platform.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONObject;
import com.rabbitmq.client.ConnectionFactory;

import cn.shiyanjun.platform.api.Context;
import cn.shiyanjun.platform.api.common.ContextImpl;
import cn.shiyanjun.platform.api.constants.TaskType;
import cn.shiyanjun.running.platform.api.MQAccessService;
import cn.shiyanjun.running.platform.component.RabbitMQAccessService;
import cn.shiyanjun.running.platform.constants.JsonKeys;
import cn.shiyanjun.running.platform.constants.RunpConfigKeys;

public class MockedMQProducer extends Thread {

	private static final Log LOG = LogFactory.getLog(MockedMQProducer.class);
	private static final String rabbitmqConfig = "rabbitmq.properties";
	private final MQAccessService taskRequestMQAccessService;
	
	public MockedMQProducer(Context context) {
		super();
		// create Rabbit MQ access service
		ResourceUtils.registerResource(rabbitmqConfig, ConnectionFactory.class);
		final ConnectionFactory connectionFactory = ResourceUtils.getResource(ConnectionFactory.class);
		String taskRequestQName = context.get(RunpConfigKeys.MQ_TASK_REQUEST_QUEUE_NAME);
		taskRequestMQAccessService = new RabbitMQAccessService(taskRequestQName, connectionFactory);
		taskRequestMQAccessService.start();
	}
	
	@Override
	public void run() {
		while(true) {
			try {
				JSONObject body = new JSONObject(true);
				body.put(JsonKeys.TASK_TYPE, TaskType.GREEN_PLUM.getCode());
				body.put(JsonKeys.TASK_TYPE_DESC, TaskType.GREEN_PLUM);
				JSONObject params = new JSONObject(true);
				params.put("a", "aaaaaaaaaa");
				params.put("b", "bbbbbbbbbb");
				params.put("c", "cccccccccc");
				body.put(JsonKeys.TASK_PARAMS, params);
				taskRequestMQAccessService.produceMessage(body.toJSONString());
				LOG.info("Message published: " + body);
				Thread.sleep(60000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	public static void main(String[] args) {
		Context context = new ContextImpl("config.properties");
		Thread producer = new MockedMQProducer(context);
		producer.start();
	}
}
