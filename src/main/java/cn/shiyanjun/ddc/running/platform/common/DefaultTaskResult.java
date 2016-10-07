package cn.shiyanjun.ddc.running.platform.common;

import cn.shiyanjun.ddc.running.platform.api.Task;
import cn.shiyanjun.ddc.running.platform.api.TaskResult;

public class DefaultTaskResult implements TaskResult {

	protected Task task;
	
	public DefaultTaskResult() {
		super();
	}

	@Override
	public void setTask(Task task) {
		this.task = task;		
	}

	@Override
	public Task getTask() {
		return task;
	}

}
