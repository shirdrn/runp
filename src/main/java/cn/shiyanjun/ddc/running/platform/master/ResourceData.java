package cn.shiyanjun.ddc.running.platform.master;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.alibaba.fastjson.JSONObject;

import cn.shiyanjun.ddc.api.constants.TaskType;
import cn.shiyanjun.ddc.running.platform.constants.JsonKeys;

public class ResourceData {

	final int taskTypeCode;
	final TaskType taskType;
	final int capacity;
	volatile int freeCount;
	String description;
	final Lock lock = new ReentrantLock();
	
	public ResourceData(TaskType type, int capacity) {
		super();
		this.taskTypeCode = type.getCode();
		this.taskType = type;
		this.capacity = capacity;
		this.freeCount = capacity;
	}
	
	public Lock getLock() {
		return lock;
	}
	
	@Override
	public int hashCode() {
		return 31 * taskType.hashCode() + 31 * capacity;
	}
	
	@Override
	public boolean equals(Object obj) {
		ResourceData other = (ResourceData) obj;
		return taskType.equals(other.taskType) && capacity == other.capacity;
	}
	
	@Override
	public String toString() {
		JSONObject data = new JSONObject(true);
		data.put(JsonKeys.TASK_TYPE_CODE, taskTypeCode);
		data.put(JsonKeys.TASK_TYPE_DESC, taskType);
		data.put(JsonKeys.CAPACITY, capacity);
		return data.toJSONString();
	}

	public int getFreeCount() {
		return freeCount;
	}

	public void incrementFreeCount() {
		this.freeCount++;
	}
	
	public void decrementFreeCount() {
		this.freeCount--;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public TaskType getTaskType() {
		return taskType;
	}

	public int getCapacity() {
		return capacity;
	}
}
