package com.zyuc.es.input.client;

import com.zyuc.es.util.Format;

public interface IndicesService {


	/**
	 * 
	 * @param service 服务名
	 * @param isSingle  是否单个索引
	 * @return
	 */
	public String obtainLatestIndexName(String service, boolean isSingle);

	/**
	 * 指定索引的日期格式并获得最新索引 并指定是否创建
	 * 如果获得的索引大于5千万，则返回一个序号自增的索引，
	 * @param service
	 * @param isSingle 是否单个索引
	 * @param format 日期格式类
	 * @return
	 */
	public String obtainLatestIndexName(String service, boolean isSingle, Format format);
}
