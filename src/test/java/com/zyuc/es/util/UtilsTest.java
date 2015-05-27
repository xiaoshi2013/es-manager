package  com.zyuc.es.util;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.DateTimeField;
import org.elasticsearch.common.joda.time.DateTimeZone;
import org.elasticsearch.common.joda.time.Duration;
import org.elasticsearch.common.joda.time.format.DateTimeFormat;
import org.elasticsearch.common.joda.time.format.DateTimeFormatter;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.zyuc.es.util.Utils;

public class UtilsTest {

	

	
	public static void testMapRemove(){
		 HashMultimap<String, Long> index_date_=HashMultimap.create();
		 
		 index_date_.put("a", 1L);
		 index_date_.put("b", 2L);
		 index_date_.put("c", 3L);
		 index_date_.put("d", 4L);
		 index_date_.put("d", 5L);
		 
			Set<String> set= index_date_.keySet();
			
			 Collection<Long> vals=index_date_.values();
			
			 vals.remove(1L);
			 vals.remove(2L);
			 vals.remove(3L);
			 vals.remove(4L);
			
			/*Set<String> set1=Sets.newHashSet();
			
			set1.add("a");
			set1.add("b");
			set.removeAll(set1);*/
			
			
			
			System.out.println(index_date_);
			
			
	}
	
	
	/*public static void testIndexRequest(){
		
		Map map=Maps.newHashMap();
		
		
		map.put("host", "aaa");
		map.put("clock", 111111L);
		map.put("@timestamp", new DateTime());
		
		
		IndexRequest request=Requests.indexRequest("test").source(map);
		
		System.out.println(Utils.encapsulateFields(request));
		
	}*/
	public static void main(String[] args) {
		
		
		
	String str="25/Mar/2015:15:39:01.412 +0800";
	
	DateTimeFormatter fmt=DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss.SSS Z");
	
	System.out.println(fmt.withLocale(Locale.ROOT).parseDateTime(str));
	
	DateTime dt=DateTime.now();
	
	
	
	}
}
