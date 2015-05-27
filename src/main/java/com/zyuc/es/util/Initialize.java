package com.zyuc.es.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;


public class Initialize {
	
	
private static Logger _LOG = Logger.getLogger(Initialize.class);

	
public static  String TIMESTAMP="@timestamp";
	
//public static   String service;
public static   String eshost;
public static   String esname;

//public static   String zkConn;
//public static   boolean useZK;



//public static final String zkpath="/indices";
//public static final String zklatestpath="/latest";

/**
 * zk 存储索引和日期数据的path
 */
//public static final String zkdatepath="/index_date";
//public static final String zknamespace="esmanage";
public static final int write_interval;
public static final int indexdate_interval;


public static  int readserverport;
public static  int indexserverport;

public static String serverhost;

public static String SERVICE="service";
public static String ISSINGLE="isSingle";
public static String FORMAT="format";

public static String ALL="all";
public static String SESSIONID="sessionId";
public static String INDICES="indices";





static {
	
		 Properties prop = new Properties();
	   String path = Thread.currentThread().getContextClassLoader().getResource("").getPath(); 
	   _LOG.info(path);
	   path=path+"client-init.properties";
		
		_LOG.info("path "+path);
	   
		
		try {
			prop.load(new FileInputStream(path));
		} catch ( IOException e) {
			
			throw new RuntimeException(e);
		}
		
		//useZK=Boolean.parseBoolean(prop.getProperty("useZK","false"));
		//service=prop.getProperty("service");
		//zkConn=prop.getProperty("zkConn");
		eshost=prop.getProperty("eshost");
		esname=prop.getProperty("esname");
		
		write_interval=Integer.parseInt(prop.getProperty("write_interval","5"));
		indexdate_interval=Integer.parseInt(prop.getProperty("indexdate_interval","10"));
		readserverport=Integer.parseInt(prop.getProperty("readserverport","7788"));
		indexserverport =Integer.parseInt(prop.getProperty("indexserverport","7789"));
		serverhost=prop.getProperty("serverhost","127.0.0.1");
		
		
		
		//_LOG.info(service);
		//_LOG.info("zookeeper: "+zkConn);
		
	

}


}
