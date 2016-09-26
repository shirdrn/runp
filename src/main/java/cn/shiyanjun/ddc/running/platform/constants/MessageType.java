package cn.shiyanjun.ddc.running.platform.constants;

public enum MessageType {

	TASK_ASSIGNMENT(1),
	TASK_PROGRESS(2),
	HEART_BEAT(3),
	WORKER_REGISTRATION(4);
	
	private int code;
	
	MessageType(int code) {
		this.code = code;
	}
	
	public int getCode() {
		return code;
	}
	
}
