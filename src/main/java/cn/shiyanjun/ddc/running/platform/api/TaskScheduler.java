package cn.shiyanjun.ddc.running.platform.api;

import java.util.Optional;

import cn.shiyanjun.ddc.api.constants.TaskType;
import cn.shiyanjun.ddc.running.platform.common.WorkOrder;

public interface TaskScheduler {

	Optional<WorkOrder> resourceOffser(TaskType taskType);
}
