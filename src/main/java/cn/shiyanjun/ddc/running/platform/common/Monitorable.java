package cn.shiyanjun.ddc.running.platform.common;

public interface Monitorable {

	void setStartTime(long startTime);
	
	long getStartTime();
	
	void setDoneTime(long doneTime);
	
	long getDoneTime();
	
	void updateLastActiveTime(long lastActiveTime);
	
	long getLastActiveTime();
}
