package cn.shiyanjun.running.platform.api;

import cn.shiyanjun.running.platform.component.worker.DefaultTask;
import cn.shiyanjun.running.platform.constants.JsonKeys;

public class DefaultRestTask extends DefaultTask implements RestTask {

	@Override
	public String getServiceUrl() {
		return params.getString(JsonKeys.SERVICE_URL);
	}
	
	@Override
	public String getPollingUrl() {
		return params.getString(JsonKeys.POLLING_URL);
	}
	
}
