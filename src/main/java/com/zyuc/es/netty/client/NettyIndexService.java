package com.zyuc.es.netty.client;

public class NettyIndexService {
	
	  protected NettyClientUtil nettyClientUtil;

	  public static enum Flag{
		  READ("read"),INDEX("index");

		private String flagName;
		  
		  
		  Flag(String flagName){
			  this.flagName=flagName;
		  }
		  
		  public String getFlagName() {
				return flagName;
			}
		  
		  
	  }
	  

	
	public NettyClientUtil getNettyClientUtil() {
		return nettyClientUtil;
	}
	  
	
	public void saveSession(String sid, String[] ss){
		this.nettyClientUtil.getIndexMap().put(sid, ss);
	}
	
}
