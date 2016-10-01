package cn.shiyanjun.ddc.running.platform.component;

import cn.shiyanjun.ddc.api.Context;
import cn.shiyanjun.ddc.api.common.AbstractComponent;
import cn.shiyanjun.ddc.api.constants.TaskType;
import cn.shiyanjun.ddc.network.common.MessageDispatcher;
import cn.shiyanjun.ddc.running.platform.api.TaskScheduler;
import cn.shiyanjun.ddc.running.platform.common.ScheduledTask;

public class TaskSchedulerImpl extends AbstractComponent implements TaskScheduler {

	private MessageDispatcher messageDispatcher;
	
	
	public TaskSchedulerImpl(Context context) {
		super(context);
	}

	@Override
	public ScheduledTask schedule(TaskType taskType) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setMessageDispatcher(MessageDispatcher messageDispatcher) {
		this.messageDispatcher = messageDispatcher;		
	}

}
