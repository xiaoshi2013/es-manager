package com.zyuc.es.util;

import java.util.Map;

import org.elasticsearch.common.collect.Maps;


public class Format {

	private  String formatter;

	public void setFormatter(String formatter) {
		this.formatter = formatter;
	}

	public String getFormatter() {
		return formatter;
	}
	
	public Format() {
		this.formatter ="yyyy-MM-dd";
	}
	

	public Format(String formatter) {
		this.formatter = formatter;
	}

	/**
	 * yyyy-MM-dd
	 */
	public static final Format format1 = new Format("yyyy-MM-dd");
	
	/**
	 * yyyyMMdd
	 */
	public static final Format format2 = new Format("yyyyMMdd");

	/**
	 * yyyy.MM.dd
	 */
	public static final Format format3 = new Format("yyyy.MM.dd");
	/**
	 * yyyy-MM
	 */
	public static final Format format4 = new Format("yyyy-MM");
	/**
	 * yyyy.MM
	 */
	public static final Format format5 = new Format("yyyy.MM");
	/**
	 * yyyyMM
	 */
	public static final Format format6 = new Format("yyyyMM");
	/**
	 * yyyy
	 */
	public static final Format format7 = new Format("yyyy");
	
	
	//public static final Format format8 = new Format("yyyy.MM.dd.HH");
	
	
	
	public static final Map<String,Format> formatMap=Maps.newHashMap();
	
	static{
		formatMap.put(format1.getFormatter(), format1);
		formatMap.put(format2.getFormatter(), format2);
		formatMap.put(format3.getFormatter(), format3);
		formatMap.put(format4.getFormatter(), format4);
		formatMap.put(format5.getFormatter(), format5);
		formatMap.put(format6.getFormatter(), format6);
		formatMap.put(format7.getFormatter(), format7);
		//formatMap.put(format8.getFormatter(), format8);
		
	}

	@Override
	public String toString() {
		return formatter;
	}

	
}
