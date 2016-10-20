package cn.shiyanjun.running.platform.api;

import java.util.Optional;

import cn.shiyanjun.platform.api.constants.TaskType;
import cn.shiyanjun.running.platform.component.master.WorkOrder;

public interface TaskScheduler {

	Optional<WorkOrder> resourceOffer(TaskType taskType);
}
