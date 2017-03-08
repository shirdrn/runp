package cn.shiyanjun.running.platform.component.worker.launchers;

import java.util.Optional;

import com.alibaba.fastjson.JSONObject;

import cn.shiyanjun.platform.api.Context;
import cn.shiyanjun.running.platform.api.Task;
import cn.shiyanjun.running.platform.api.TaskResult;
import cn.shiyanjun.running.platform.common.AbstractTaskLauncher;

public class JavaTaskLauncher extends AbstractTaskLauncher {

	public JavaTaskLauncher(Context context) {
		super(context);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected Optional<TaskResult> runTask(Task task) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected Optional<Task> createTask(JSONObject params) {
		// TODO Auto-generated method stub
		return null;
	}

}
