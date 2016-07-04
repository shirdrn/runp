package cn.shiyanjun.ddc.running.platform.common;

public interface MqMessageAccessor {

	String fetch(String queue) throws Exception;
}
