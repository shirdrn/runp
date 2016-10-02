package cn.shiyanjun.ddc.running.platform.common;

public class WorkOrder {

	private String targetWorkerId;
	private WorkerInfo workerInfo;

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
	
}
