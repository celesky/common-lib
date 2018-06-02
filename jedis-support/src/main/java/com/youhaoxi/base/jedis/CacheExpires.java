/**
 * 
 */
package com.youhaoxi.base.jedis;


import org.apache.commons.lang3.time.DateUtils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * 缓存过期时间
 * @description <br>
 */
public class CacheExpires {
	
	public final static long IN_1MIN = 60;
	
	public final static long IN_3MINS = 60 * 3; 
	
	public final static long IN_5MINS = 60 * 5;

	public final static long IN_1HOUR = 60 * 60;
	
	public final static long IN_1DAY = IN_1HOUR * 24;
	
	public final static long IN_1WEEK = IN_1DAY * 7;
	
	public final static long IN_1MONTH = IN_1DAY * 30;
	
	/**
	 * 当前时间到今天结束相隔的秒
	 * @return
	 */
	public static long todayEndSeconds(){
		//Date curTime = new Date();
		//String day = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
		//String endOfDayStr = day+"23:59:59";
		LocalDateTime now = LocalDateTime.now();
		int year = now.getYear();
		int month = now.getMonthValue();
		int day = now.getDayOfMonth();

		LocalDateTime endOfDay = LocalDateTime.of(year,month,day,23,59,59);
		long remainSecond = endOfDay.toEpochSecond(ZoneOffset.ofHours(8))-LocalDateTime.now().toEpochSecond(ZoneOffset.ofHours(8));
		if(remainSecond>0){
			return remainSecond;
		}else{
			return 0;
		}
	}

	public static void main(String[] args) {
		CacheExpires.todayEndSeconds();
	}
	
}
