package cn.shiyanjun.running.platform.component.master;

import cn.shiyanjun.platform.api.constants.TaskType;

public class WorkOrder {

	private String targetWorkerId;
	private WorkerInfo workerInfo;
	private TaskType taskType;

	public String getTargetWorkerId() {
		return targetWorkerId;
	}

	public void setTargetWorkerId(String targetWorkerId) {
		this.targetWorkerId = targetWorkerId;
	}

	public WorkerInfo getWorkerInfo() {
		return workerInfo;
	}

	public void setWorkerInfo(WorkerInfo workerInfo) {
		this.workerInfo = workerInfo;
	}

	public TaskType getTaskType() {
		return taskType;
	}

	public void setTaskType(TaskType taskType) {
		this.taskType = taskType;
	}
	
}
