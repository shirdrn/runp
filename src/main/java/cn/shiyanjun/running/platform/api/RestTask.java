package cn.shiyanjun.running.platform.api;

public interface RestTask extends Task {

	String getServiceUrl();
	String getPollingUrl();
}
