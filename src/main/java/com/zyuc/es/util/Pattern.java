package com.zyuc.es.util;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.format.DateTimeFormat;

/**
 * format1 yyyyMMdd<br>
 * format2 yyyy-MM-dd<br>
 * format3 yyyy.MM.dd<br>
 * format4 yyyy-MM<br>
 * format5 yyyy.MM<br>
 * format6 yyyyMM<br>
 * format7 yyyyM<br>
 * 
 * 
 * 
 * @author zhanglei
 * 
 */
public class Pattern {

	private static final Logger _LOG = Logger.getLogger(Pattern.class);

	/**
	 * service-->后缀模板
	 */
	static final Map<String, String> PATTERN_CACHE = Maps.newHashMap();

	// 没有时间后缀的 也会自动创建序号索引
	// public static final Set<String> noSuffix_indices=Sets.newHashSet();
	public static final Set<String> no_timetamp_indices = Sets.newHashSet();

	static final String LATEST = "latest"; // 标识 最新索引
	static final String MERGE = "merge";
	static final String START = "start"; // 标识 索引开始时间前缀
	static final String END = "end"; // 标识 索引结束时间的前缀

	public static String matchPattern(String dateStr) {

		String str = StringUtils.remove(dateStr, ".");
		str = StringUtils.remove(str, "-");

		// System.out.println("matchPattern "+dateStr+"  "+str);

		if (!StringUtils.isNumeric(str)) {
			return StringUtils.EMPTY;
		}
		
		if(str.length()>8){
			return StringUtils.EMPTY;
		}

		/*
		 * if (StringUtils.countMatches(dateStr, ".") == 3 && str.length() ==
		 * 10) { return Format.format8.getFormatter(); }
		 */
		if (StringUtils.countMatches(dateStr, ".") == 2) {
			return "yyyy.MM.dd";
		}

		else if (StringUtils.countMatches(dateStr, "-") == 2) {
			return "yyyy-MM-dd";
		}

		else if (StringUtils.countMatches(dateStr, ".") == 1) {
			return "yyyy.MM";
		}

		else if (StringUtils.countMatches(dateStr, "-") == 1) {
			return "yyyy-MM";
		}

		if (dateStr.length() == 8 && dateStr.indexOf("-") == -1 && dateStr.indexOf(".") == -1) {
			return "yyyyMMdd";
		}

		else if (dateStr.length() == 4) {
			return "yyyy";
		} else if (dateStr.length() == 6) {
			return "yyyyMM";

		}

		return StringUtils.EMPTY;
	}

	/**
	 * 根据日期名获得时间
	 * 
	 * @param index
	 * @param service
	 * @return
	 */
	static DateTime toDateTime(String index, String service) {
		DateTime dt = null;
		try {
			String str = StringUtils.remove(index, service);
			str = Utils.normalization(str);
			dt = DateTime.parse(str, DateTimeFormat.forPattern(Pattern.matchPattern(str)));

			/*
			 * if(dt.getHourOfDay()==0){ dt=dt.withHourOfDay(23); }
			 */

			dt = dt.withMinuteOfHour(59);
			dt = dt.withSecondOfMinute(59);
		} catch (Exception e) {
			_LOG.error("index:" + index + " service:" + service);
			_LOG.error("error "+e.getMessage());

		}

		return dt;
	}

	static String toFormat(String index, String service) {
		String str = StringUtils.remove(index, service);
		str = Utils.normalization(str);
		String template = Pattern.matchPattern(str);

		return template;

	}

	/**
	 * 第二天的时间
	 * 
	 * @return
	 */
	public static DateTime dayTime() {
		DateTime dt = new DateTime();
		dt = dt.withHourOfDay(23);
		dt = dt.withMinuteOfHour(59);
		dt = dt.withSecondOfMinute(59);
		dt.plusDays(1);

		return dt;
	}

/*	static String normalization(String dateStr) {

		if (dateStr.startsWith("-") || dateStr.startsWith("_")) {
			dateStr = dateStr.substring(1);

		}

		// 判断序号 和索引名本身就带下划线的
		if (dateStr.lastIndexOf("_") > 0) {

			if (StringUtils.substringAfterLast(dateStr, "_").length() <= 2) {
				dateStr = StringUtils.substringBeforeLast(dateStr, "_");

			}

		}

		return dateStr;

	}*/

	/**
	 * 有可能和索引名一样 没有时间后缀的返回索引名 不论是下划线还是横杠都去掉 返回索引名
	 * 
	 * @param indexName
	 * @param dt
	 * @return service name
	 * @throws Exception
	 */
	public static String parsing(String indexName) {

		DateTime dt = DateTime.now();
		int year = dt.getYear() + 1;
		int n = -1;
		for (int i = 0; i < 20; i++) {
			n = indexName.indexOf(year + "");

			if (n < 0) {
				year = year - 1;

				n = indexName.indexOf(year + "");
			} else {
				break;
			}
		}

		if (n < 0) {
			// 没有时间后缀
			// System.out.println("Index name No template: "+indexName);

			// noSuffix_indices.add(indexName);
			// 没有时间后缀的直接返回索引名

			String service = StringUtils.substringBeforeLast(indexName, "_");
			String no = StringUtils.substringAfterLast(indexName, "_");
			if (no.length() <= 2 && no.length() >= 1) {
				int num = Integer.parseInt(no);

				return service;
			}

			return indexName;

		}

		// 有时间后缀
		String suffix = indexName.substring(indexName.indexOf(year + ""));

		String service = indexName.substring(0, indexName.indexOf(suffix));
		if (service.endsWith("-") || service.endsWith("_")) {
			service = service.substring(0, service.length() - 1);
		}

		return service;
	}

	/***
	 * 判断增量索引 包括有时间和没时间后缀的 时间是第一条件
	 * 
	 * @param indices
	 * @return 最新索引名
	 */

	public static String determineIncremental(String[] indices, String service) {

		if(indices.length==1){
			return indices[0];
		}
		List<String> list = Lists.newArrayList();
		for (int i = 0; i < indices.length; i++) {
			
			boolean b = byTime(indices[i]);
			if (b) {
				list.add(indices[i]);
			}

		}

		String index = "";

		if (list.size() > 0) {
			long time = 0;
			for (int i = 0; i < list.size(); i++) {
				try {
					DateTime dt = toDateTime(list.get(i), service);
					
					if (dt!=null && dt.getMillis() > time) {
						time = dt.getMillis();
						index = list.get(i);
					}

				} catch (Exception e) {
					_LOG.error("error---- index: "+list.get(i)+" servive: "+service+"  "+e.getMessage());
				}
			
			}

		}

		if (!index.equals("")) {
			List<String> list1 = Lists.newArrayList();
			long t = toDateTime(index, service).getMillis(); // 临时的最大时间

			for (String index_ : list) {
				DateTime dt=toDateTime(index_, service);
				if(dt!=null){
					long t1 = dt.getMillis();
					if (t1 == t) { // 如果有相同时间 再根据序号比较一次
						list1.add(index_);
					}
				}
			

			}
			
			if (list1.size() > 1) {
				//_LOG.debug("The same date " + service + " " + list1);
				index = compare(list1.toArray(new String[list1.size()]), service);
				_LOG.debug("The return index " + service + " " + index);

				return index;

			}

		}
		
		
		else{
			index = compare(indices, service);
			
			return index;
		}

		
		
		return index;

	}

	/**
	 * 根据序号比较
	 * 
	 * @param indices
	 * @param service
	 * @return 返回序号最大的索引名
	 */
	static String compare(String[] indices, String service) {
		String name = "";
		int l = 0;

		for (int i = 0; i < indices.length; i++) {
			String index1 = indices[i];

			String str = StringUtils.remove(index1, service);

			String suffix;
			if (str.startsWith("_") || str.startsWith("-")) {
				suffix = str.substring(1);

			} else {
				suffix = str;

			}

			if (suffix.indexOf("_") == -1 && suffix.length() <= 2 && suffix.length() > 0) {

				int n = Integer.parseInt(suffix);
				if (n > l) {
					l = n;
					name = index1;
				}
				continue;
			}

			String no = StringUtils.substringAfterLast(suffix, "_");
			if (no.length() > 0) {
				int n = Integer.parseInt(no);
				if (n > l) {
					l = n;
					name = index1;

				}
			}
		}

		return name;
	}

	static boolean byTime(String indexName) {
		DateTime dt = DateTime.now();
		int year = dt.getYear() + 1;
		int n = -1;
		for (int i = 0; i < 20; i++) {
			n = indexName.indexOf(year + "");

			if (n < 0) {
				year = year - 1;

				n = indexName.indexOf(year + "");
			} else {
				break;
			}
		}

		if (n < 0) {
			return false;
		}
		return true;

	}
	
	
	/**
	 * 创建增量索引 加上日期  如果同一个日期的加上序号
	 * 如果之前没有时间 则直接加序号 替换latest
	 * @param service
	 * @param pattern
	 * @param indexName
	 * @return
	 * @throws Exception
	 */
	public static String creatIncrementalIndex(String service,String oldIndex) throws Exception{
		_LOG.info("service "+service +" oldIndex "+oldIndex);
		
		if(StringUtils.isBlank(service) || StringUtils.isBlank(oldIndex)){
			throw new Exception("index service or indexName is null ");
		}
	
		
		
		String newIndex="";
		
		
		// 没有时间后缀的 直接加序号
		
		if(service.equals(oldIndex)){
			newIndex=Utils.noSuffix(oldIndex);

		}else{
			// 有时间后缀 acve_2014_1 c_ve-2014 这种要处理 同一日期的加序号 不同的不加
			
			newIndex=Utils.dealUnderline(service, oldIndex);
	
		}
		
		_LOG.info("new indexName "+newIndex +" old indexName "+oldIndex);
			return newIndex;
		
		
	
	}
	

	/**
	 * 时间间隔
	 * 
	 * @author zhanglei
	 * 
	 */
	public static class Interval {

		public static final Interval DAY = new Interval("1d");
		// public static final Interval WEEK = new Interval("1w");
		public static final Interval MONTH = new Interval("1M");
		// public static final Interval QUARTER = new Interval("1q");
		public static final Interval YEAR = new Interval("1y");

		public static Interval days(int days) {
			return new Interval(days + "d");
		}

		/*
		 * public static Interval weeks(int weeks) { return new Interval(weeks +
		 * "w"); }
		 */

		private final String expression;

		private Interval(String expression) {
			this.expression = expression;
		}

		@Override
		public String toString() {
			return expression;
		}
	}

}
