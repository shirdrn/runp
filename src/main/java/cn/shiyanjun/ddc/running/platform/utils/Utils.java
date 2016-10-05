package cn.shiyanjun.ddc.running.platform.utils;

import java.util.Arrays;
import java.util.stream.Collectors;

import cn.shiyanjun.ddc.running.platform.constants.MessageType;

public class Utils {

	public static Integer[] toIntegerArray(MessageType... messageTypes) {
		Integer[] types = new Integer[messageTypes.length];
		Arrays.stream(messageTypes)
			.map(mt -> mt.getCode())
			.collect(Collectors.<Integer>toList())
			.toArray(types);
		return types;
	}
	
}
