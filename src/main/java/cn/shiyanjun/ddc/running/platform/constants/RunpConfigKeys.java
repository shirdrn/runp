package cn.shiyanjun.ddc.running.platform.constants;

public interface RunpConfigKeys {

	String MQ_TASK_REQUEST_QUEUE_NAME = "mq.task.request.queue.name";
	String MQ_TASK_RESULT_QUEUE_NAME = "mq.task.result.queue.name";
	
	String MASTER_ID = "master.id";
	String WORKER_ID = "worker.id";
	String WORKER_HOST = "worker.host";
	String RESOURCE_TYPES = "worker.resource.types";
	String WORKER_TASK_LAUNCHER_PACKAGE = "worker.task.launcher.package";
	String WORKER_HEARTBEAT_INTERVALMILLIS = "worker.heartbeat.intervalMillis";
}
