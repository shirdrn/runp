package cn.shiyanjun.ddc.running.platform.common;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import cn.shiyanjun.ddc.api.Context;
import cn.shiyanjun.ddc.api.constants.TaskType;
import cn.shiyanjun.ddc.running.platform.constants.RunpConfigKeys;
import cn.shiyanjun.ddc.running.platform.worker.ClientConnectionManager;

public final class WorkerContext extends RunpContext {

	private static final Log LOG = LogFactory.getLog(WorkerContext.class);
	private final AtomicLong messageidGenerator = new AtomicLong();
	private final String workerHost;
	private final Map<TaskType, Integer> resourceTypes = Maps.newHashMap();
	private ClientConnectionManager clientConnectionManager;
	
	public WorkerContext(Context context) {
		super();
		this.context = context;
		workerHost = context.get(RunpConfigKeys.WORKER_HOST);
		parseConfiguredResources();
	}

	private void parseConfiguredResources() {
		String[] types = context.getStringArray(RunpConfigKeys.RESOURCE_TYPES, null);
		Preconditions.checkArgument(types != null, "Configured resource types shouldn't be null");
		
		for(String t : types) {
			String[] type = t.split(":");
			String typeCode = type[0];
			String typeValue = type[1];
			Optional<TaskType> taskType = TaskType.fromCode(Integer.parseInt(typeCode));
			taskType.ifPresent(tp -> {
				resourceTypes.put(tp, Integer.parseInt(typeValue));
				LOG.info("Resource supported: typeCode=" + typeCode + ", taskType=" + tp + ", typeValue=" + typeValue);
			});
		}
	}
	
	public String getWorkerHost() {
		return workerHost;
	}

	public Map<TaskType, Integer> getResourceTypes() {
		return Collections.unmodifiableMap(resourceTypes);
	}

	public ClientConnectionManager getClientConnectionManager() {
		return clientConnectionManager;
	}

	public void setClientConnectionManager(ClientConnectionManager clientConnectionManager) {
		this.clientConnectionManager = clientConnectionManager;
	}

	public AtomicLong getMessageidGenerator() {
		return messageidGenerator;
	}
	
	
}
