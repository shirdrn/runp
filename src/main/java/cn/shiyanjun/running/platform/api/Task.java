package cn.shiyanjun.running.platform.api;

import com.alibaba.fastjson.JSONObject;

import cn.shiyanjun.platform.api.common.Id;
import cn.shiyanjun.platform.api.common.Typeable;
import cn.shiyanjun.platform.api.constants.TaskStatus;

public interface Task extends Id<Long>, Typeable {

	void setParams(JSONObject params);
	JSONObject getParams();
	
	void setTaskStatus(TaskStatus taskStatus);
    TaskStatus getTaskStatus();
    
    void setResult(String result);
    String getResult();
}
