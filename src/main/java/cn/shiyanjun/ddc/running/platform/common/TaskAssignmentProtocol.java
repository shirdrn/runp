package cn.shiyanjun.ddc.running.platform.common;

import cn.shiyanjun.ddc.network.common.RpcMessage;

public interface TaskAssignmentProtocol {

	/**
	 * Assign a task based on the kind of {@link RpcMessage}.
	 * @param msg
	 */
	void assign(RpcMessage msg);
}
