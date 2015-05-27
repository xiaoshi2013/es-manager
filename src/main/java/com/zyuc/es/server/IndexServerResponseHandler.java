package com.zyuc.es.server;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.format.DateTimeFormat;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.buffer.DynamicChannelBuffer;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.zyuc.es.util.Format;
import com.zyuc.es.util.Initialize;
import com.zyuc.es.util.Pattern;

public class IndexServerResponseHandler extends SimpleChannelUpstreamHandler {
	private static final Logger LOG = Logger.getLogger(IndexServerResponseHandler.class);

	private LatestIndicesServiceThread latestIndicesServiceThread;

	public IndexServerResponseHandler(LatestIndicesServiceThread latestIndicesServiceThread) {
		this.latestIndicesServiceThread = latestIndicesServiceThread;
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {

		ChannelBuffer buffer = (ChannelBuffer) e.getMessage();
		// System.out.println( buffer.readableBytes());
		String msg = buffer.toString(Charset.forName("UTF-8"));

		Map map = JSON.parseObject(msg, Map.class);

		Object o = map.get(Initialize.SERVICE);

		String sid = map.get(Initialize.SESSIONID).toString();

		String error = "";
		Map<String,Object> res = new HashMap();
		if (o == null) {
			error = "session " + sid + " service is null";

			res.put("error", error);
			res.put(Initialize.SESSIONID, sid);

		} else {
			String service = o.toString();
			service = StringUtils.removeEnd(service, "-");

			Format format=null;
			
			boolean isSingle = map.get(Initialize.ISSINGLE)==null ? false : Boolean.parseBoolean( map.get(Initialize.ISSINGLE).toString());
			Object oFormat = map.get(Initialize.FORMAT);
			if(oFormat!=null){
				format=JSON.parseObject(oFormat.toString(), Format.class);
			}
		
			String index=obtainLatestIndexName(service, isSingle, format);
			String[] ss = null;
			if(index!=null){
				ss=new String[]{index};
			}
			
			res.put(Initialize.SESSIONID, sid);
			res.put(Initialize.INDICES, ss);
	
		}

		String resstr = JSON.toJSONString(res, SerializerFeature.WriteMapNullValue);

		// LOG.debug("write ----- "+resstr);
		try {
			ChannelBuffer message = new DynamicChannelBuffer(1024);

			message.writeBytes(ChannelBuffers.wrappedBuffer(resstr.getBytes("UTF-8")));
			ChannelFuture future = e.getChannel().write(message);

			future.syncUninterruptibly();

		} catch (UnsupportedEncodingException e1) {
			e1.printStackTrace();
		}
	}

	private String obtainLatestIndexName(String service, boolean isSingle, Format format) {

		while (latestIndicesServiceThread.getServices() == null || latestIndicesServiceThread.getLatest() == null) {
			// _LOG.debug("services is empty...");
			try {
				TimeUnit.MILLISECONDS.sleep(200);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		if (Pattern.no_timetamp_indices.contains(service) && isSingle) {
			return service;

		}
		
		if(isSingle){
			System.out.println("---- "+isSingle+"  "+service);
			return service;
		}
		// 之后都是非单例的

		service = StringUtils.removeEnd(service, "-");
		service = StringUtils.removeEnd(service, "_");
		
		

		if (!isSingle && format!=null) {
			
			String index_ = service + "-" + DateTime.now().toString(DateTimeFormat.forPattern(format.getFormatter()));
			latestIndicesServiceThread.getLatest().put(service, index_);
			return index_;
			/*if (latestIndicesServiceThread.getIndex_stats().get(index_) != null) {
				// System.out.println("MiniIndexService.obtainLatestIndexName() "+index_stats.get(index).getPrimaries().docs.getCount());
				if (latestIndicesServiceThread.getIndex_stats().get(index_).getPrimaries().docs.getCount() < ServicesAndIndices.MAX_DOC
						&& latestIndicesServiceThread.getIndex_stats().get(index_).getPrimaries().getStore().getSizeInBytes() < 
						ServicesAndIndices.MAX_BYTES) {
					
					latestIndicesServiceThread.getLatest().put(service, index_);

					return index_;
				}

				index_ = Pattern.determineIncremental(latestIndicesServiceThread.getService_index().get(service).toArray(new String[0]), service);

				try {
					String newindex = Pattern.creatIncrementalIndex(service, index_);
					latestIndicesServiceThread.getLatest().put(service, newindex);

					return newindex;
				} catch (Exception e) {
					LOG.error("error", e);
					return null;

				}

			} else {
				latestIndicesServiceThread.getLatest().put(service, index_);
				return index_;
			}*/
		


		}
	
		
		// 非单例 但是format为空
		String index = latestIndicesServiceThread.getLatest().get(service);

		if (StringUtils.isNotBlank(index)) {
			

			if (latestIndicesServiceThread.getIndex_stats().get(index) != null) {
				//LOG.debug("----index  "+index);
				if (latestIndicesServiceThread.getIndex_stats().get(index).getPrimaries().docs.getCount() < ServicesAndIndices.MAX_DOC
						&& latestIndicesServiceThread.getIndex_stats().get(index).getPrimaries().getStore().getSizeInBytes() < 
						ServicesAndIndices.MAX_BYTES) {
					return index;
				}

				index = Pattern.determineIncremental(latestIndicesServiceThread.getService_index().get(service).toArray(new String[0]), service);

				if (latestIndicesServiceThread.getIndex_stats().get(index) != null) {
					if (latestIndicesServiceThread.getIndex_stats().get(index).getPrimaries().docs.getCount() < ServicesAndIndices.MAX_DOC
							&& latestIndicesServiceThread.getIndex_stats().get(index).getPrimaries().getStore().getSizeInBytes() < 
							ServicesAndIndices.MAX_BYTES) {
						return index;
					}
				}
				
				try {
					String newindex = Pattern.creatIncrementalIndex(service, index);
					latestIndicesServiceThread.getLatest().put(service, newindex);
					
					return newindex;
				} catch (Exception e) {
					LOG.error("error", e);
					return null;

				}

			} else {
				return index;
			}

		}
		else{
			if (format == null) {
				format = Format.format1;
			}

			String index_ = service + "-" + DateTime.now().toString(DateTimeFormat.forPattern(format.getFormatter()));
			//System.out.println("---------new index: "+index_);

			latestIndicesServiceThread.getLatest().put(service, index_);

			return index_;
		}

	
	
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
		// Close the connection when an exception is raised.
		e.getCause().printStackTrace();
		e.getChannel().close();
	}

}
