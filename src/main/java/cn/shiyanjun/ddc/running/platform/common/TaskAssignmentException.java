package cn.shiyanjun.ddc.running.platform.common;

public class TaskAssignmentException extends Exception {

	private static final long serialVersionUID = 1L;
	
	public TaskAssignmentException() {
		super();
	}
	
	public TaskAssignmentException(String message) {
        super(message);
    }
	
	public TaskAssignmentException(String message, Throwable cause) {
        super(message, cause);
    }

}
