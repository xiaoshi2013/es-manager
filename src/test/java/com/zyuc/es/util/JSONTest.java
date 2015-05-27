package com.zyuc.es.util;

import java.util.Arrays;
import java.util.Date;
import java.util.Map;

import org.elasticsearch.common.joda.time.DateTime;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.google.common.collect.Maps;

public class JSONTest {
	
	public static void main(String[] args) {
	Map<String,Object> map=Maps.newHashMap();
		
		map.put("service", "test");
		map.put("from", null);
		map.put("to", null);
		map.put("all", true);
		
		
	String str=  JSON.toJSONString(map,SerializerFeature.WriteMapNullValue);
	
	System.out.println(str);
	
	Map map1=JSON.parseObject(str,Map.class);
	System.out.println(((Boolean)map.get("all"))==true);
	
	
    String session=Thread.currentThread().getName()+"_"+System.nanoTime();

    System.out.println(session);
    
    String[] ss= new String[0];
    System.out.println(Arrays.toString(ss));
    
    System.out.println(new Date(0).toLocaleString());
    
	}

}
