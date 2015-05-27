package com.zyuc.es.server;

import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.collect.Multimap;

public class ServicesAndIndices {

	/* public static volatile Set<String>  services;

	 public	static volatile Map<String,String> latest;
	
	 public	static volatile Map<String,IndexStats> index_stats;

	 public	static volatile Multimap<String, String> service_index;*/
		
		
	 public static final String ERROR="error";
	 
	 public static final String SUCCESS="success"; 
	
	 public static final String EMPTY="empty";
	
	 public static final int  MAX_DOC=3_0_000_000;
	 
	// public static final int  MAX_DOC=2000;
	 
	 
	 public static final long  MAX_BYTES=30L*1024*1024*1024L;
	// public static final long  MAX_BYTES=1024*1024L;
	 
	 
	 static volatile boolean stop=false;
	 
	 

}
