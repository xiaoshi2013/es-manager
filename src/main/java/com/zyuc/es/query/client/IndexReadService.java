package com.zyuc.es.query.client;

public interface IndexReadService {

	/**
	 * 起止时间 毫秒
	 * @param service
	 * @param from
	 * @param to
	 * @return
	 */
	public String[] getIndexName(String service, long from, long to);

	/**
	 * 传入日期字符串
	 * @param service
	 * @param date
	 * @return
	 */
	public String[] getIndexName(String service, String date);

	/**
	 * 几天之前的差值
	 * @param service
	 * @param period
	 * @return
	 */
	public String[] getIndexName(String service, int period);

	/**
	 * 返回此服务所有索引
	 * @param service
	 * @return
	 */
	public String[] getIndexNameAll(String service);

}
