package cn.shiyanjun.running.platform.utils;

import cn.shiyanjun.platform.api.Context;
import cn.shiyanjun.platform.api.common.AbstractObjectFactory;
import cn.shiyanjun.platform.api.utils.ReflectionUtils;
import cn.shiyanjun.running.platform.api.TaskLauncher;

public class TaskLauncherFactoryImpl extends AbstractObjectFactory<TaskLauncher> {

	@SuppressWarnings("unchecked")
	@Override
	protected TaskLauncher createObject(Context context, String objectClazz) throws Exception {
		Class<TaskLauncher> clazz = (Class<TaskLauncher>) ReflectionUtils.newClazz(objectClazz);
		return ReflectionUtils.newInstance(clazz, TaskLauncher.class, new Object[] { context });
	}
}
