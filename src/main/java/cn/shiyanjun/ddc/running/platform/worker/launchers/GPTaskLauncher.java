package cn.shiyanjun.ddc.running.platform.worker.launchers;

import com.alibaba.fastjson.JSONObject;

import cn.shiyanjun.ddc.api.Context;
import cn.shiyanjun.ddc.running.platform.api.Task;
import cn.shiyanjun.ddc.running.platform.api.TaskResult;
import cn.shiyanjun.ddc.running.platform.common.AbstractTaskLauncher;
import cn.shiyanjun.ddc.running.platform.common.DefaultTaskResult;
import cn.shiyanjun.ddc.running.platform.worker.DefaultTask;

public class GPTaskLauncher extends AbstractTaskLauncher {

	public GPTaskLauncher(Context context) {
		super(context);
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