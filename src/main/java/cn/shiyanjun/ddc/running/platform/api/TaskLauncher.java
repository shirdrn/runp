package cn.shiyanjun.ddc.running.platform.api;

import com.alibaba.fastjson.JSONObject;

import cn.shiyanjun.ddc.api.LifecycleAware;
import cn.shiyanjun.ddc.api.common.Typeable;
import cn.shiyanjun.ddc.network.common.PeerMessage;
import cn.shiyanjun.ddc.network.common.RunnableMessageListener;

/**
 * Launch a {@link Task} instance in the <code>Job Running Platform</code>
 * and monitor the status for collecting execution result.
 * 
 * @author yanjun
 */
public interface TaskLauncher extends Typeable, LifecycleAware {

	void launchTask(long taskId, JSONObject params);
	
	void setTaskProgressReporter(RunnableMessageListener<PeerMessage> taskProgressReporter);
}
