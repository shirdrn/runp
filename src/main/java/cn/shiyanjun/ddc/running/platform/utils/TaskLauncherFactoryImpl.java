package cn.shiyanjun.ddc.running.platform.utils;

import cn.shiyanjun.ddc.api.Context;
import cn.shiyanjun.ddc.api.JobPlugin;
import cn.shiyanjun.ddc.api.common.AbstractObjectFactory;
import cn.shiyanjun.ddc.api.utils.ReflectionUtils;
import cn.shiyanjun.ddc.running.platform.api.TaskLauncher;

public class TaskLauncherFactoryImpl extends AbstractObjectFactory<TaskLauncher> {

	@SuppressWarnings("unchecked")
	@Override
	protected TaskLauncher createObject(Context context, String objectClazz) throws Exception {
		Class<JobPlugin> clazz = (Class<JobPlugin>) ReflectionUtils.newClazz(objectClazz);
		return ReflectionUtils.newInstance(clazz, TaskLauncher.class, new Object[] { context });
	}
}
