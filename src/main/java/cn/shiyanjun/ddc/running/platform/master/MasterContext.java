package cn.shiyanjun.ddc.running.platform.master;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import cn.shiyanjun.ddc.api.Context;
import cn.shiyanjun.ddc.api.constants.TaskType;
import cn.shiyanjun.ddc.running.platform.api.TaskScheduler;
import cn.shiyanjun.ddc.running.platform.common.RunpContext;

public final class MasterContext extends RunpContext {

	private static final Log LOG = LogFactory.getLog(MasterContext.class);
	private final ConcurrentMap<String, WorkerInfo> workers = Maps.newConcurrentMap();
	private final ConcurrentMap<String, Map<TaskType, ResourceData>> resources = Maps.newConcurrentMap();
	private TaskScheduler taskScheduler;
	
	public MasterContext(Context context) {
		super(context);
		peerId = getMasterId();
		Preconditions.checkArgument(peerId != null);
	}
	
	public Optional<WorkerInfo> getWorker(String workerId) {
		return Optional.ofNullable(workers.get(workerId));
	}
	
	public synchronized void updateWorker(String workerId, WorkerInfo workerInfo) {
		workers.putIfAbsent(workerId, workerInfo);
	}
	
	public synchronized void releaseResource(String workerId, TaskType taskType) {
		Map<TaskType, ResourceData> rdMap = resources.get(workerId);
		if(rdMap != null) {
			ResourceData rd = rdMap.get(taskType);
			if(rd != null) {
				try {
					if(rd.getLock().tryLock()) {
						rd.incrementFreeCount();
					}
				} finally {
					rd.getLock().unlock();
				}
			}
		}
	}
	
	public List<String> getAvailableWorkers(TaskType taskType) {
		List<String> availableWorkers = Lists.newArrayList();
		for(String workerId : resources.keySet()) {
			Map<TaskType, ResourceData> resource = resources.get(workerId);
			ResourceData rd = resource.get(taskType);
			if(rd != null && rd.getFreeCount() > 0) {
				availableWorkers.add(workerId);
			}
		}
		return availableWorkers;
	}
	
	public Optional<ResourceData> getResource(String workerId, TaskType taskType) {
		ResourceData rd = null;
		Map<TaskType, ResourceData> resource = resources.get(workerId);
		if(resource != null) {
			rd = resource.get(taskType);
		} else {
			LOG.warn("Resource not found: workerId=" + workerId);
		}
		return Optional.ofNullable(rd);
	}
	
	public void removeResource(String workerId) {
		resources.remove(workerId);
		workers.remove(workerId);
	}
	
	public synchronized void updateResource(String workerId, ResourceData resource) {
		Map<TaskType, ResourceData> rds = resources.get(workerId);
		if(rds == null) {
			rds = Maps.newHashMap();
			resources.putIfAbsent(workerId, rds);
		}
		TaskType taskType = resource.getTaskType();
		ResourceData oldRes = rds.get(taskType);
		if(oldRes == null) {
			rds.put(taskType, resource);
		} else {
			try {
				oldRes.getLock().lock();
				if(resource.equals(oldRes)) {
					LOG.warn("Resource already registered: wokerId=" + workerId + ", resource=" + resource);
				} else {
					rds.put(taskType, resource);
					LOG.info("Resource updated: wokerId=" + workerId + ", oldRes=" + oldRes + ", newRes=" + resource);
				}
			} finally {
				oldRes.getLock().unlock();
			}
		}
	}
	
	public TaskScheduler getTaskScheduler() {
		return taskScheduler;
	}

	public void setTaskScheduler(TaskScheduler taskScheduler) {
		this.taskScheduler = taskScheduler;
	}
	
}
