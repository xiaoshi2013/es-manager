/*package com.zyuc.es.query.client;


import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.format.DateTimeFormat;

import com.zyuc.es.util.Pattern;
import com.zyuc.es.util.Utils;

public class ZKIndexReadService implements IndexReadService {

	private static final Logger LOG = Logger.getLogger(ZKIndexReadService.class);

	private QueryClient esQueryClient;
	public ZKIndexReadService(String zkConn){
		try {
			esQueryClient=new QueryClient(zkConn);
		} catch (Exception e) {
			LOG.error("error", e);
		}
		
	}
	
	public String[] getIndexName(String service,long from, long to){
		try {
			String[] names= esQueryClient.getIndexNames(service, from, to);
			if(names==null){
				names=new String[]{service+"*"};
			}
			LOG.debug("getIndexNameBytime: "+ArrayUtils.toString(names));
			return names;
		} catch (Exception e) {
			 LOG.error("error", e);
		}
	
		return null;
		
	}
	

	*//**
	 * 
	 * @param service
	 * @param date  日期 可以yyyy-MM-dd or yyyy-MM or yyyy
	 * @return
	 *//*
	public String[] getIndexName(String service, String date) {
		try {
			
			String pattern= Pattern.matchPattern(date);
			if(pattern.equals("")){
				return null;
			}
			DateTime dt=DateTime.parse(date,DateTimeFormat.forPattern(pattern));
			long to=0;
			if(pattern.endsWith("y")){
				to=dt.plusYears(1).getMillis()-1;
			}
			else if(pattern.endsWith("M")){
				to=dt.plusMonths(1).getMillis()-1;
			}
			else {
				to=dt.getMillis();
			}
			
			
			
			
			String[] names= esQueryClient.getIndexNames(service, dt.getMillis(), to);

			if(names==null){
				names=new String[]{service+"*"};
			}
			LOG.debug("getIndexNameBytime: "+ArrayUtils.toString(names));
			return names;
		} catch (Exception e) {
			 LOG.error("error", e);
		}
	
		return null;
	}
	
	
	public String[] getIndexNameAll(String service){
		String[] names= esQueryClient.getIndexNames(service);
		if(names==null){
			names=new String[]{service+"*"};
		}
		
		return names;
		
	}
	
	public String[] getIndexName(String service,int period){
		try {
			Tuple<Long,Long> tuple=Utils.generateFromAndToDate(period);
			String[] names= esQueryClient.getIndexNames(service, tuple.v1(), tuple.v2());
			if(names==null){
				names=new String[]{service+"*"};
			}
			LOG.debug("getIndexNameBytime: "+ArrayUtils.toString(names));
			return names;
		} catch (Exception e) {
			 LOG.error("error", e);
		}
	
		return null;
	}
	

}
*/