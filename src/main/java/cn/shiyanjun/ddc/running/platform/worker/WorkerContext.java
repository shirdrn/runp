package cn.shiyanjun.ddc.running.platform.worker;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import cn.shiyanjun.ddc.api.Context;
import cn.shiyanjun.ddc.api.common.ObjectFactory;
import cn.shiyanjun.ddc.api.constants.TaskType;
import cn.shiyanjun.ddc.running.platform.api.TaskLauncher;
import cn.shiyanjun.ddc.running.platform.common.RunpContext;
import cn.shiyanjun.ddc.running.platform.constants.RunpConfigKeys;
import cn.shiyanjun.ddc.running.platform.utils.TaskLauncherFactoryImpl;

public final class WorkerContext extends RunpContext {

	private static final Log LOG = LogFactory.getLog(WorkerContext.class);
	private final AtomicLong messageidGenerator = new AtomicLong();
	private final String workerHost;
	private final Map<TaskType, Integer> resourceTypes = Maps.newHashMap();
	private final Map<TaskType, String> launcherClasses = Maps.newHashMap();
	private ClientConnectionManager clientConnectionManager;
	private final String taskLauncherPackage;
	private final ObjectFactory<TaskLauncher> taskLauncherFactory;
	
	public WorkerContext(Context context) {
		super(context);
		workerHost = context.get(RunpConfigKeys.WORKER_HOST);
		peerId = context.get(RunpConfigKeys.WORKER_ID);
		Preconditions.checkArgument(peerId != null);
		Preconditions.checkArgument(workerHost != null);
		
		taskLauncherPackage = context.get(RunpConfigKeys.WORKER_TASK_LAUNCHER_PACKAGE);
		Preconditions.checkArgument(taskLauncherPackage != null);
		
		parseConfiguredResources();
		
		taskLauncherFactory = new TaskLauncherFactoryImpl();
	}

	private void parseConfiguredResources() {
		String[] types = context.getStringArray(RunpConfigKeys.RESOURCE_TYPES, null);
		Preconditions.checkArgument(types != null, "Configured resource types shouldn't be null");
		
		for(String t : types) {
			String[] type = t.split(":");
			String typeCode = type[0];
			String typeValue = type[1];
			String clazz = type[2];
			Optional<TaskType> taskType = TaskType.fromCode(Integer.parseInt(typeCode));
			taskType.ifPresent(tp -> {
				String launcherClazz = String.join(".", taskLauncherPackage, clazz);
				resourceTypes.put(tp, Integer.parseInt(typeValue));
				launcherClasses.put(tp, launcherClazz);
				LOG.info("Resource supported: typeCode=" + typeCode + ", taskType=" + tp + ", typeValue=" + typeValue + ", launcherClazz=" + launcherClazz);
			});
		}
	}
	
	public Optional<String> getTaskLauncherClass(TaskType taskType) {
		return Optional.ofNullable(launcherClasses.get(taskType));
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

	public ObjectFactory<TaskLauncher> getTaskLauncherFactory() {
		return taskLauncherFactory;
	}
	
	
}
