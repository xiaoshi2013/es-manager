package com.zyuc.es.input.client;

import java.nio.charset.Charset;
import java.util.Map;

import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.zyuc.es.netty.client.NettyIndexService;
import com.zyuc.es.util.Initialize;

public class IndexClientHandler  extends SimpleChannelUpstreamHandler {
	
	private static final Logger LOG = Logger.getLogger(IndexClientHandler.class);
	
	private NettyIndexService service;
	
	public IndexClientHandler(NettyIndexService service){
		this.service=service;
	}
	
	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		
	}

	@Override
	public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		// TODO Auto-generated method stub
		super.channelDisconnected(ctx, e);
	}

	@Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
      
    	ChannelBuffer buffer=(ChannelBuffer) e.getMessage();
		   
    	String msg = buffer.toString(Charset.forName("UTF-8"));

		    
		    
		   // LOG.debug("messageReceived------ "+msg);
		    
		 Map<String,Object> map=JSON.parseObject(msg,Map.class);
		Object o=  map.get(Initialize.SESSIONID);
		
		String sid=o.toString();
		
	 	 if(map.get("error")!=null){
	 		 LOG.error(map.get("error"));
	 	 }
	 	 else{
	 		Object  oindices=map.get(Initialize.INDICES);
			if(oindices!=null && oindices instanceof JSONArray){
				
				String[] ss=((JSONArray)oindices).toArray(new String[0]);
				service.saveSession(sid, ss);
			}

	 	 }
	
	 	 //LOG.debug(JSON.toJSONString( MiniIndexReadService.concurrentMap,SerializerFeature.WriteMapNullValue));
		    
    }

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)  {
		try {
			LOG.error("error",e.getCause());
			this.service.getNettyClientUtil().error=true;
			LOG.warn("--- "+e.getCause().getMessage());
		} catch (Exception e2) {
			LOG.error("exception", e2);
			
		}
		
		
		
	}
	
	 
}
