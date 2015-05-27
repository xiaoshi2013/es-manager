package com.zyuc.es.query.client;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.format.DateTimeFormat;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.zyuc.es.netty.client.NettyClientUtil;
import com.zyuc.es.netty.client.NettyIndexService;
import com.zyuc.es.util.Initialize;
import com.zyuc.es.util.Pattern;
import com.zyuc.es.util.Utils;

public class MiniIndexReadService extends NettyIndexService  implements IndexReadService {
	private static final Logger LOG = Logger.getLogger(MiniIndexReadService.class);
	 
	private static volatile MiniIndexReadService singleton = null;

	
	static final int BASELINE=2010;
	

	
	private MiniIndexReadService(){
		nettyClientUtil=new NettyClientUtil(this, Flag.READ);
	}
	
	public static MiniIndexReadService getInstance(){
		if (singleton == null) {
			synchronized (MiniIndexReadService.class) {
				singleton=new MiniIndexReadService();
			}
		}
		return singleton;
	}
	
	
	public void doStart(){
		this.nettyClientUtil.doStart();


		
	}
	
	public void doStop(){
		this.nettyClientUtil.doStop();
		
	}
	

	
	public String[] getIndexName(String service,long from, long to){
		try {
			String[] names= getIndexNames(service, from, to);
			if(names==null ||names.length==0){
				names=new String[]{service+"*"};
			}
			return names;
		} catch (Exception e) {
			 LOG.error("error", e);
		}
	
		return null;
		
	}
	
	/**
	 * 
	 * @param service
	 * @param date  日期 可以yyyy-MM-dd or yyyy-MM or yyyy
	 * @return
	 */
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
			
			
			String[] names= getIndexNames(service, dt.getMillis(), to);

			if(names==null || names.length==0){
				names=new String[]{service+"*"};
			}
			//LOG.debug("getIndexNameBytime: "+ArrayUtils.toString(names));
			return names;
		} catch (Exception e) {
			 LOG.error("error", e);
		}
	
		return null;
	}
	
	/**
	 * @param service
	 * @param period 天数
	 * @return
	 */
	public String[] getIndexName(String service,int period){
		try {
			org.elasticsearch.common.collect.Tuple<Long,Long> tuple=Utils.generateFromAndToDate(period);
			String[] names= getIndexNames(service, tuple.v1(), tuple.v2());
			if(names==null ||names.length==0){
				names=new String[]{service+"*"};
			}
			return names;
		} catch (Exception e) {
			 LOG.error("error", e);
		}
	
		return null;
	}
	
	/**
	 * 获得服务下全部索引
	 * @param service
	 * @return
	 */
	public String[] getIndexNameAll(String service) {
		return obtainIndices(service, -1, -1, true);
	}

	
	private String[] obtainIndices(String service,long from,long to,boolean all){
		long count=this.nettyClientUtil.increment();
		Map<String,Object> map=Maps.newHashMap();
		service = StringUtils.removeEnd(service, "-");
		service = StringUtils.removeEnd(service, "_");
		
		map.put("service", service);
		
		if(all){
			map.put(Initialize.ALL, true);

		}
		else{
			if(from < 0){
				from =0;
			}
		
			if(to < 0){
				to=0;
			}
			
			map.put("from", from);
			map.put("to", to);
			map.put(Initialize.ALL, false);
			LOG.debug(new DateTime(from)+" "+new DateTime(to));


		}
			
	    String session="Read-"+Thread.currentThread().getName()+"_"+count;
		map.put(Initialize.SESSIONID,session);

		this.nettyClientUtil.getIndexMap().put(session, new String[]{});
		String str=  JSON.toJSONString(map,SerializerFeature.WriteMapNullValue);		
		
		ChannelBuffer message;
		String[] indices=null;
		try {
			message = ChannelBuffers.wrappedBuffer(str.getBytes("UTF-8"));
		    ChannelFuture future = nettyClientUtil.getChannel().write(message);
		    future.syncUninterruptibly();
		   // LOG.debug(concurrentMap.keySet());
    		//LOG.debug(session +concurrentMap.get(session));

		    
		    long start=System.currentTimeMillis();
		    while(nettyClientUtil.getIndexMap().get(session)==null||nettyClientUtil.getIndexMap().get(session).length==0){
		    	try {
					TimeUnit.MILLISECONDS.sleep(100);
					if((System.currentTimeMillis()-start)>1000*120){
						LOG.info("timeout 120s session" +session +" is null Discard");
						break;
					}
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		    }
	
		   
		} catch (UnsupportedEncodingException e1) {
			LOG.error("error",e1);
		}
		finally{
			indices=nettyClientUtil.getIndexMap().remove(session);
		}
		LOG.debug(session +Arrays.toString(indices));
		return indices;
	}
	
	private String[] getIndexNames(String service, long from, long to) {
		
		return obtainIndices(service, from, to, false);

	}
	
}
