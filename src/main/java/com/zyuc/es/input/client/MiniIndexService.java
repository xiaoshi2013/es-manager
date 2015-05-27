package com.zyuc.es.input.client;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.common.collect.Maps;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.zyuc.es.netty.client.NettyClientUtil;
import com.zyuc.es.netty.client.NettyIndexService;
import com.zyuc.es.util.Format;
import com.zyuc.es.util.Initialize;




public class MiniIndexService extends NettyIndexService  implements IndicesService {

	private static final Logger LOG = Logger.getLogger(MiniIndexService.class);

	
	   
	private static volatile MiniIndexService singleton = null;

    
	public static  MiniIndexService getInstance() {
		if (singleton == null) {
			synchronized (MiniIndexService.class) {
				singleton=new MiniIndexService();
			}
		}
		return singleton;
	}
	private MiniIndexService()  {
		
			nettyClientUtil=new NettyClientUtil(this, Flag.INDEX);
		
	}
	
	public void doStart(){
		this.nettyClientUtil.doStart();
	}
	
	public void doStop(){
		this.nettyClientUtil.doStop();
	}
	
	public String obtainLatestIndexName(String service,boolean isSingle) {
		return obtainLatestIndexName(service,isSingle, null);
	}
	
	public String obtainLatestIndexName(String service,boolean isSingle, Format format) {
		long count=this.nettyClientUtil.increment();
		Map<String,Object> map=Maps.newHashMap();
		service = StringUtils.removeEnd(service, "-");
		service = StringUtils.removeEnd(service, "_");
		map.put("service", service);
		if (format != null) {
			map.put(Initialize.FORMAT, format);
		}
		map.put("isSingle", isSingle);

		String session = "Index-" + Thread.currentThread().getName() + "_" + count;
		map.put(Initialize.SESSIONID, session);

		this.nettyClientUtil.getIndexMap().put(session, new String[] {});

		String str = JSON.toJSONString(map, SerializerFeature.WriteMapNullValue);

		ChannelBuffer message;
		String[] indices = null;
			try {
				message = ChannelBuffers.wrappedBuffer(str.getBytes("UTF-8"));
			    ChannelFuture future = nettyClientUtil.getChannel().write(message);
			    future.syncUninterruptibly();

			    long start=System.currentTimeMillis();
			    while(nettyClientUtil.getIndexMap().get(session)==null||nettyClientUtil.getIndexMap().get(session).length==0){
			    	try {
						TimeUnit.MILLISECONDS.sleep(100);
						if((System.currentTimeMillis()-start)>1000*120){
							LOG.info("timeout 120s session" +session +" is null Discard");
							break;
						}
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
			    }
		
			    indices=nettyClientUtil.getIndexMap().remove(session);
				//LOG.debug(session +Arrays.toString(indices));
				if(indices==null ) return null;
				return indices[0];
			} catch (Exception e1) {
				LOG.error("error--",e1);
				nettyClientUtil.error=true;
				return null;
			}
			

	
		
	}


	
/*	public void close() {
		try {
			ClientUtil.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	
	}*/




}
