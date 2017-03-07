package cn.shiyanjun.running.platform.component.worker;

import java.util.Optional;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONObject;
import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;

import cn.shiyanjun.platform.api.Context;
import cn.shiyanjun.running.platform.api.DefaultRestTask;
import cn.shiyanjun.running.platform.api.RestTask;
import cn.shiyanjun.running.platform.api.Task;
import cn.shiyanjun.running.platform.api.TaskResult;
import cn.shiyanjun.running.platform.common.AbstractTaskLauncher;
import cn.shiyanjun.running.platform.common.DefaultTaskResult;

public class RestPollingTaskLauncher extends AbstractTaskLauncher {

	private static final Log LOG = LogFactory.getLog(RestPollingTaskLauncher.class);
	private final OkHttpClient client = new OkHttpClient();
	private final MediaType mediaType = MediaType.parse("application/json");
	
	public RestPollingTaskLauncher(Context context) {
		super(context);
	}

	@Override
	protected Optional<TaskResult> runTask(Task task) {
		final RestTask t = (RestTask) task;
		Request request = buildRequest(t, task.getParams());
		Optional<TaskResult> result = Optional.empty();
		try {
			Response response = client.newCall(request).execute();
			if(response != null && response.isSuccessful()) {
				String res = response.body().string();
				t.setResult(res);
				LOG.info("Submitted response: " + res);
				// release all system resources
				response.body().close();
				TaskResult taskResult = new DefaultTaskResult();
				taskResult.setTask(t);
				taskResult.setTask(t);
				result = Optional.of(taskResult);
			}
		} catch (Exception e) {
			LOG.error("Failed to run task: ", e);
		}
		return result;
	}
	
	private Request buildRequest(RestTask task, JSONObject params) {
		String url = task.getServiceUrl();
		RequestBody body = RequestBody.create(mediaType, params.toJSONString());
		Request request = new Request.Builder()
				.url(url)
				.post(body)
				.addHeader("content-type", "application/json")
				.addHeader("cache-control", "no-cache")
				.build();
		return request;
	}

	@Override
	protected Optional<Task> createTask(JSONObject params) {
		RestTask task = new DefaultRestTask();
		task.setParams(params);
		return Optional.ofNullable(task);
	}

}
