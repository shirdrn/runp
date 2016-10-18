package cn.shiyanjun.running.platform.worker.launchers;

import com.alibaba.fastjson.JSONObject;

import cn.shiyanjun.platform.api.Context;
import cn.shiyanjun.running.platform.api.Task;
import cn.shiyanjun.running.platform.api.TaskResult;
import cn.shiyanjun.running.platform.common.AbstractTaskLauncher;
import cn.shiyanjun.running.platform.common.DefaultTaskResult;
import cn.shiyanjun.running.platform.worker.DefaultTask;

public class JavaTaskLauncher extends AbstractTaskLauncher {

	public JavaTaskLauncher(Context context) {
		super(context);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected TaskResult runTask(Task task) {
		TaskResult result = new DefaultTaskResult();
		result.setTask(task);
		return result;
	}

	@Override
	protected Task createTask(JSONObject params) {
		Task task = new DefaultTask();
		task.setParams(params);
		return task;
	}

	

}
