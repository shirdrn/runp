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
import cn.shiyanjun.ddc.running.platform.master.MasterMessageDispatcher.ResourceData;

public class TaskSchedulerImpl extends AbstractComponent implements TaskScheduler {

	private static final Log LOG = LogFactory.getLog(TaskSchedulerImpl.class);
	private final MasterContext masterContext;
	
	
	public TaskSchedulerImpl(MasterContext masterContext) {
		super(masterContext.getContext());
		this.masterContext = masterContext;
	}

	@Override
	public Optional<WorkOrder> schedule(TaskType taskType) {
		Optional<WorkOrder> scheduledTask = Optional.empty();
		List<String> workers = masterContext.getAvailableWorkers(taskType);
		LOG.debug("Available workers: " + workers);
		
		Collections.shuffle(workers);
		for(String workerId : workers) {
			ResourceData rd = masterContext.getResource(workerId, taskType);
			LOG.debug("Check resource data: workerId=" + workerId + ", resource=" + rd);
			try {
				if(rd.getLock().tryLock(3000, TimeUnit.MILLISECONDS) && rd.getFreeCount() > 0) {
					rd.decrementFreeCount();
					final WorkOrder wo = new WorkOrder();
					wo.setTargetWorkerId(workerId);
					wo.setWorkerInfo(masterContext.getWorker(workerId));;
					scheduledTask = Optional.of(wo);
					break;
				} else {
					continue;
				}
			} catch(InterruptedException e) {
				e.printStackTrace();
			} finally {
				rd.getLock().unlock();
			}
		}
		return scheduledTask;
	}

}
