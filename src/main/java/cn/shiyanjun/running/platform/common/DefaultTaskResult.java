package cn.shiyanjun.running.platform.common;

import cn.shiyanjun.running.platform.api.Task;
import cn.shiyanjun.running.platform.api.TaskResult;

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
