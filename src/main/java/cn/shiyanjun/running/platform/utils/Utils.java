package cn.shiyanjun.running.platform.utils;

import java.util.Arrays;
import java.util.stream.Collectors;

import cn.shiyanjun.running.platform.constants.MessageType;

public class Utils {

	public static Integer[] toIntegerArray(MessageType... messageTypes) {
		return Arrays.stream(messageTypes)
			.map(mt -> mt.getCode())
			.collect(Collectors.<Integer>toList())
			.toArray(new Integer[messageTypes.length]);
	}
	
}
