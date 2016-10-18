package cn.shiyanjun.running.platform.constants;

import java.util.Optional;
import java.util.stream.Stream;

public enum MessageType {

	TASK_ASSIGNMENT(1),
	ACK_TASK_ASSIGNMENT(2),
	TASK_PROGRESS(3),
	HEART_BEAT(4),
	WORKER_REGISTRATION(5),
	ACK_WORKER_REGISTRATION(6),
	
	SENT_ACK(11),
	RECEIVED_ACK(12);
	
	private int code;
	
	MessageType(int code) {
		this.code = code;
	}
	
	public int getCode() {
		return code;
	}
	
	public static Optional<MessageType> fromCode(int code) {
		return Stream.of(values()).filter((tasktype) -> {
			return tasktype.code == code;
		}).findAny();
	}
	
}
