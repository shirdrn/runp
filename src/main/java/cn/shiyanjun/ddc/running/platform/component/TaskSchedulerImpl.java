package cn.shiyanjun.ddc.running.platform.component;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.shiyanjun.ddc.api.common.AbstractComponent;
import cn.shiyanjun.ddc.api.constants.TaskType;
import cn.shiyanjun.ddc.running.platform.api.TaskScheduler;
import cn.shiyanjun.ddc.running.platform.common.MasterContext;
import cn.shiyanjun.ddc.running.platform.common.WorkOrder;
import cn.shiyanjun.ddc.running.platform.common.WorkerInfo;
import cn.shiyanjun.ddc.running.platform.master.MasterMessageDispatcher.ResourceData;

public class TaskSchedulerImpl extends AbstractComponent implements TaskScheduler {

	private static final Log LOG = LogFactory.getLog(TaskSchedulerImpl.class);
	private final MasterContext masterContext;
	
	
	public TaskSchedulerImpl(MasterContext masterContext) {
		super(masterContext.getContext());
		this.masterContext = masterContext;
	}

	@Override
	public Optional<WorkOrder> resourceOffser(TaskType taskType) {
		Optional<WorkOrder> scheduledTask = Optional.empty();
		List<String> availableWorkers = masterContext.getAvailableWorkers(taskType);
		LOG.debug("Available workers: " + availableWorkers);
		
		if(!availableWorkers.isEmpty()) {
			Collections.shuffle(availableWorkers);
			scheduledTask = availableWorkers.stream().findFirst().<WorkOrder>map(workerId -> {
				Optional<ResourceData> result = masterContext.getResource(workerId, taskType);
				WorkOrder wo = new WorkOrder();
				result.ifPresent(rd -> {
					try {
						if(rd.getLock().tryLock(3000, TimeUnit.MILLISECONDS) && rd.getFreeCount()>0) {
							rd.decrementFreeCount();
							wo.setTargetWorkerId(workerId);
							Optional<WorkerInfo> wi = masterContext.getWorker(workerId);
							wo.setWorkerInfo(wi.get());
						}
					} catch(InterruptedException e) {
					} finally {
						rd.getLock().unlock();
					}
				});
				return wo;
			});
		}
		return scheduledTask;
	}

}
