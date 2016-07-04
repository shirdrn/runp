package cn.shiyanjun.ddc.running.platform.common;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class WorkerInfo {

	private final ConcurrentMap<Integer, WorkerInfo> workers = new ConcurrentHashMap<>();
}
