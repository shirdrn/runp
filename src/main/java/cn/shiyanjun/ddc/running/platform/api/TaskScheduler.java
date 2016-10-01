package cn.shiyanjun.ddc.running.platform.api;

import cn.shiyanjun.ddc.api.constants.TaskType;
import cn.shiyanjun.ddc.network.common.MessageDispatcher;
import cn.shiyanjun.ddc.running.platform.common.ScheduledTask;

public interface TaskScheduler {

	void setMessageDispatcher(MessageDispatcher messageDispatcher);
	ScheduledTask schedule(TaskType taskType);
}
