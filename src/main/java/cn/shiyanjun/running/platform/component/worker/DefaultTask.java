package cn.shiyanjun.running.platform.component.worker;

import com.alibaba.fastjson.JSONObject;

import cn.shiyanjun.platform.api.constants.TaskStatus;
import cn.shiyanjun.running.platform.api.Task;

public class DefaultTask implements Task {

	private Long id;
	private int type;
	protected JSONObject params;
	private TaskStatus status;
	private String result;
	
	@Override
	public void setId(Long id) {
		this.id = id;
	}

	@Override
	public Long getId() {
		return id;
	}

	@Override
	public void setType(int type) {
		this.type = type;
	}

	@Override
	public int getType() {
		return type;
	}

	@Override
	public void setParams(JSONObject params) {
		this.params = params;
	}

	@Override
	public JSONObject getParams() {
		return params;
	}

	@Override
	public void setTaskStatus(TaskStatus status) {
		this.status = status;
	}

	@Override
	public TaskStatus getTaskStatus() {
		return status;
	}

	@Override
	public void setResult(String result) {
		this.result = result;
	}

	@Override
	public String getResult() {
		return result;
	}

}
