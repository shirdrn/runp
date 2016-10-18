package cn.shiyanjun.running.platform.utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Time {

	private static final String DF = "yyyy-MM-dd HH:mm:ss";
	
	public static long now() {
		return System.currentTimeMillis();
	}
	
	public static String readableTime(long ts) {
		DateFormat df = new SimpleDateFormat(DF);
		return df.format(new Date(ts));
	}
	
}
