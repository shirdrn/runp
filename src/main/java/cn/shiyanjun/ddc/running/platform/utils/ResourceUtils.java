package cn.shiyanjun.ddc.running.platform.utils;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.Maps;
import com.rabbitmq.client.ConnectionFactory;

import cn.shiyanjun.ddc.api.Context;
import cn.shiyanjun.ddc.api.common.ContextImpl;

public class ResourceUtils {

	private static final ConcurrentMap<Class<?>, Object> pooledInstances = Maps.newConcurrentMap();
	private static final ConcurrentMap<Class<?>, String> pooledConfigurations = Maps.newConcurrentMap();
	private static final Map<Class<?>, ResourceBuilder<?>> builders = Maps.newHashMap();
	
	static {
		// register resource builder instances
		builders.put(ConnectionFactory.class, new RabbitConnectionFactoryBuilder());
	}
	
	@SuppressWarnings({ "unchecked" })
	public static synchronized <T> void registerResource(String config, Class<T> resourceClazz) {
		ResourceBuilder<?> builder = builders.get(resourceClazz);
		T resource = (T) builder.build(config);
		pooledInstances.putIfAbsent(resourceClazz, resource);
		pooledConfigurations.putIfAbsent(resourceClazz, config);
	}
	
	@SuppressWarnings({ "unchecked" })
	public static <T> T getResource(Class<T> resourceClazz) {
		return (T) pooledInstances.get(resourceClazz);
	}
	
	
	interface ResourceBuilder<T> {
		T build(String config);
	}
	
	private static abstract class AbstractResourceBuilder {
		
		protected String getString(Context context, String key, String defaultValue) {
			return context.get(key, defaultValue);
		}
		
		protected int getInt(Context context, String key, int defaultValue) {
			return context.getInt(key, defaultValue);
		}
	}
	
	private static final class RabbitConnectionFactoryBuilder extends AbstractResourceBuilder implements ResourceBuilder<ConnectionFactory> {

		private static String RABBITMQ_HOST = "rabbitmq.host";
		private static String RABBITMQ_PORT = "rabbitmq.port";
		private static String RABBITMQ_USERNAME = "rabbitmq.username";
		private static String RABBITMQ_PASSWORD = "rabbitmq.password";
		private static String RABBITMQ_CONNECT_TIMEOUT = "rabbitmq.connect.timeout";
		private static String RABBITMQ_AUTOMATIC_RECOVERY = "rabbitmq.automatic.recovery";
		
		@Override
		public ConnectionFactory build(String config) {
			final Context context = new ContextImpl(config);
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost(getString(context, RABBITMQ_HOST, "localhost"));
			factory.setPort(getInt(context, RABBITMQ_PORT, 5672));
			factory.setUsername(getString(context, RABBITMQ_USERNAME, null));
			factory.setPassword(getString(context, RABBITMQ_PASSWORD, null));
			factory.setConnectionTimeout(context.getInt(RABBITMQ_CONNECT_TIMEOUT, 30000));
			factory.setAutomaticRecoveryEnabled(context.getBoolean(RABBITMQ_AUTOMATIC_RECOVERY, true));
			return factory;
		}
		
	}
	
}
