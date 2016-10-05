package cn.shiyanjun.ddc.running.platform.api;

import cn.shiyanjun.ddc.api.LifecycleAware;
import cn.shiyanjun.ddc.api.Task;
import cn.shiyanjun.ddc.api.common.Typeable;

/**
 * Launch a {@link Task} instance in the <code>Job Running Platform</code>
 * and monitor the status for collecting execution result.
 * 
 * @author yanjun
 */
public interface TaskLauncher extends Typeable, LifecycleAware {

	void launchTask(int taskId);
	
}
