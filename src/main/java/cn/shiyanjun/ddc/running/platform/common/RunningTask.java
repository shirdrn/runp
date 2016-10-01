package cn.shiyanjun.ddc.running.platform.common;

import cn.shiyanjun.ddc.api.Task;
import cn.shiyanjun.ddc.running.platform.api.Monitorable;

public class RunningTask implements Monitorable {

	private Task task;
	private long startTime;
	private long doneTime;
	private long lastActiveTime;
	private Exception cause;

	@Override
	public void setStartTime(long startTime) {
		this.startTime = startTime;		
	}

	@Override
	public long getStartTime() {
		return startTime;
	}

	@Override
	public void setDoneTime(long doneTime) {
		this.doneTime = doneTime;
	}

	@Override
	public long getDoneTime() {
		return doneTime;
	}

	@Override
	public void updateLastActiveTime(long lastActiveTime) {
		this.lastActiveTime = lastActiveTime;		
	}

	@Override
	public long getLastActiveTime() {
		return lastActiveTime;
	}

	public Task getTask() {
		return task;
	}

	public void setTask(Task task) {
		this.task = task;
	}

	public Exception getCause() {
		return cause;
	}

	public void setCause(Exception cause) {
		this.cause = cause;
	}
}
