package com.zyuc.es.query.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.CharUtils;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.miscellaneous.KeepWordFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.index.analysis.StandardHtmlStripAnalyzer;
import org.elasticsearch.index.fielddata.RamUsage;
import org.elasticsearch.index.query.MatchQueryBuilder.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache.Key;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram.Bucket;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram.Interval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Order;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.zyuc.es.analyzer.MyUrlAnalyzer;
import com.zyuc.es.util.ClientUtil;
import com.zyuc.es.util.Initialize;
import com.zyuc.es.util.Tuple;
import com.zyuc.es.util.Utils;

public class QueryTest {
	
	private static final Logger LOG = Logger.getLogger(QueryTest.class);

	
	public  void query(){
		
		
		SearchResponse res= ClientUtil.getClient().prepareSearch("msg-2015-03-18")
				//.setTypes("program")
				.setSize(0)
				//.setRouting("1")
				.addAggregation(AggregationBuilders.histogram("date1")
						.field("@timestamp_d").interval(100))
		.execute().actionGet();
		
		System.out.println(res.getTook());

		
	//	Histogram agg = res.getAggregations().get("date1");

		//System.out.println(agg.getBuckets().size());
		
	}
	
	public  void query1(){
		
		long start=System.currentTimeMillis();
		
		SearchResponse res= ClientUtil.getClient().prepareSearch("log-*")
				.setSearchType(SearchType.COUNT)
				.setSize(0)
				//.setRouting("1")
				.addAggregation(AggregationBuilders.dateHistogram("date1")
						.field("@timestamp").interval(Interval.DAY)
						//.subAggregation(AggregationBuilders.terms("term1").field("@host"))
						
						)
		.execute().actionGet();
		
		System.out.println((System.currentTimeMillis()-start)+" ms");
		System.out.println(res);

		
	//	Histogram agg = res.getAggregations().get("date1");

		//System.out.println(agg.getBuckets().size());
		
	}
	
	
	public  void query2(){
		
		long start=System.currentTimeMillis();
		
		SearchRequestBuilder req=ClientUtil.getClient().prepareSearch("log-*")
				.setQuery(QueryBuilders.matchQuery("@message", "Session").operator(Operator.AND))
				.setSize(5);
		
		
		System.out.println(req);
				
		SearchResponse res=req.execute().actionGet();
		
		System.out.println((System.currentTimeMillis()-start)+" ms");
		System.out.println(res);

		
		
	//	Histogram agg = res.getAggregations().get("date1");

		//System.out.println(agg.getBuckets().size());
		
	}
	
	public  void testKryo(){
		 final int BASELINE=2010;

		
		ClientUtil.initClient(Initialize.esname, Initialize.eshost);
		
		 Map<String, Long> index_docs=Maps.newHashMap();
		 
		IndicesStatsResponse res = ClientUtil.getClient().admin().indices()
				.prepareStats().execute().actionGet();
		 Map<String, IndexStats> status=res.getIndices();
		Set<String> indices_ = status.keySet();
		
		 HashMultimap<String, Long> index_date_=HashMultimap.create();
		    DateTime now=new DateTime();
			Long start=System.currentTimeMillis();

			for (String index : indices_) {
				
				long docs=status.get(index).getTotal().getDocs().getCount();
				if(docs==0){
					index_docs.put(index, docs);
					continue;
				}
				if(index_docs.get(index)==null || index_docs.get(index).longValue()!=docs){
					index_docs.put(index, docs);
				}
				else if(index_docs.get(index).longValue()==docs){// doc没变的 略过
					continue;
				}
				
				SearchResponse searchres= ClientUtil.getClient().prepareSearch(index)
						.setSize(0)
						.setSearchType(SearchType.COUNT)
						.setPreference("_local")
						.setRouting("4")
						.addAggregation(AggregationBuilders.dateHistogram("day")
								.interval(Interval.DAY).field("@timestamp")
								.preZone("Asia/Shanghai")
								.preZoneAdjustLargeInterval(true)
								.order(Order.KEY_ASC))
								.execute().actionGet();
						
						//System.out.println(index +" took "+ searchres.getTook());
						DateHistogram  agg=  searchres.getAggregations().get("day");
						 Collection<? extends Bucket> coll= agg.getBuckets();
						 for (Bucket bucket : coll) {
								 
								 DateTime dt=new DateTime(bucket.getKeyAsDate().getMillis());
								 if(dt.getYear()<=BASELINE || dt.getYear()>now.getYear()){
									// LOG.error("error Discarded "+dt+" index:"+index);
									 continue;
								 }
								
								 index_date_.put(index, bucket.getKeyAsDate().getMillis());
							}
						
			}
		 
		
		//	System.out.println("QueryTest.testKryo() "+index_date_);
		DateTime dt=new DateTime();
		File f = new File("index_date.bin.test.tmp");
		f.delete();
		Kryo kryo = new Kryo();
		kryo.setReferences(false);
		kryo.register(com.zyuc.es.util.Tuple.class);
		Output output;
		try {
			output = new Output(new FileOutputStream(f));
			
			Map<String, Collection<Long>> map_=new HashMap();
		
			Set<String> keys = index_date_.keySet();
			for (String key : keys) {
				
				Collection<Long> values =Lists.newArrayList();
				values.addAll(index_date_.get(key));
				map_.put(key, values);
				
			}
			
			Tuple tuple = new Tuple(dt.getMillis(), map_);
			kryo.writeObject(output, tuple);
			output.flush();
			output.close();
			String newname= "index_date.test.bin";
			Utils.renameFile(f.getName() ,newname);
			
			
			Map<String, Collection<Long>> index_date_1 = deserialization();

			System.out.println("QueryTest.testKryo() "+(System.currentTimeMillis()-start));
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		
	}
	
	
	private  synchronized Map<String, Collection<Long>> deserialization() {
		long start = System.currentTimeMillis();
		Kryo kryo = new Kryo();
		kryo.setReferences(false);
		kryo.register(com.zyuc.es.util.Tuple.class);
		File f = new File("index_date.test.bin");
		if (f.exists()) {
			try {
				Input input = new Input(new FileInputStream(f));
				Map<String, Collection<Long>> index_date_ = null;
				while (!input.eof()) {
					Tuple tuple = kryo.readObject(input, Tuple.class);
					index_date_ = (Map<String, Collection<Long>>) tuple.v2();

				}
				input.close();

				//LOG.info("Deserialization index_date.test.bin " + (System.currentTimeMillis() - start) + " ms");
				//LOG.info("index_date.test.bin  " + index_date_.keySet());
				//LOG.info("index_date.test.bin  " + index_date_.values());
				return index_date_;
			} catch (FileNotFoundException e) {

				e.printStackTrace();
			}

		}
		return null;
	}
	
	
	public void testAnalyzer(){
		
		
		 
		String str="1370247257.612      1 116.226.218.185 TCP_IMS_HIT/304 292 GET http://www.icbc.com.cn/SiteCollectionDocuments/" +
				"ICBC/Resources/ADResources/AD_ICBC/2013%e5%b9%b4/%e6%80%bb%e8%a1%8c/%e7%94%b5%e5%ad%90%e9%93%b6%e8%a1%8c%e9%83%a8/%e4%b8%80%e5%ad%a3%" +
				"e5%ba%a6/0227%e8%bd%ac%e8%b4%a6%e6%b1%87%e6%ac%be%e6%89%8b%e7%bb%ad%e8%b4%b9%e6%9c%80%e4%bd%8e2%e6%8a%98%e6%b4%bb%e5%8a%a8%e5%ae%a3%" +
				"e4%bc%a0/%e8%bd%ac%e8%b4%a6%e6%b1%87%e6%ac%be%e6%89%8b%e7%bb%ad%e8%b4%b9%e6%9c%80%e4%bd%8e2%e6%8a%98%e6%b4%bb%e5%8a%a8_230_160_0315.jpg" +
				"  - NONE/- image/jpeg \"http://www.icbc.com.cn/icbc/\" " +
				"\"Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0; KB974488)\" filtervertion=D3F6C6AF-4D7E-403e-8D5A-9CCFC452F040;%20ismobile=false;%20ASP.NET_SessionId=lck2wy45rhwfnpygsc1vqpen";
		
		String str1= "10.231.72.74 - - [17/Mar/2015:08:03:09 +0800] \"GET /nms/Common/Img/icon_node_navigator.gif HTTP/1.1\" 304 - 0";
		
		CharArraySet set=new CharArraySet(Version.LUCENE_48,16,true);
		set.add('-');
		Analyzer analyzer=new MyUrlAnalyzer(Version.LUCENE_48, set);
		//Analyzer analyzer=new StandardAnalyzer(Version.LUCENE_48, CharArraySet.EMPTY_SET);
		TokenStream stream=	null;
		try {
			 stream=analyzer.tokenStream("msg", str);
			 
			 
	
			 
			 stream.reset();
			while(stream.incrementToken()){
				CharTermAttribute att = stream.getAttribute(CharTermAttribute.class);

				System.out.println("term:"+att);
			}
			  stream.end();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally {
            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException e) {
                    // ignore
                }
            }
           
                analyzer.close();
            
        }
	}
	

	public static void main(String[] args) {
	
/*System.out.println("QueryTest.main() "+(char)32);
System.out.println("QueryTest.main() "+(char)33);
System.out.println("QueryTest.main() "+(char)34);
System.out.println("QueryTest.main() "+(char)35);
System.out.println("QueryTest.main() "+(char)36);
System.out.println("QueryTest.main() "+(char)37);
System.out.println("QueryTest.main() "+(char)38);
System.out.println("QueryTest.main() "+(char)39);
System.out.println("QueryTest.main() "+(char)40);
System.out.println("QueryTest.main() "+(char)41);
System.out.println("QueryTest.main() "+(char)42);
System.out.println("QueryTest.main() "+(char)43);
System.out.println("QueryTest.main() "+(char)44);
System.out.println("QueryTest.main() "+(char)45);
System.out.println("QueryTest.main() "+(char)46);
System.out.println("QueryTest.main() "+(char)47);
System.out.println("QueryTest.main() "+(char)58);*/

/*for (int i = 0; i < 128; i++) {
=======
for (int i = 0; i < 128; i++) {
>>>>>>> dc35a5cc21e03c852f89348b0f8381dc331f9117
	
	
	char[] chars=Character.toChars(i);
	System.out.println(i+"--"+CharUtils.unicodeEscaped( chars[0])+" "+chars[0]);
	
	

	
}*/


		
/*
		 String PUNCTUATION = " !\"',;:.-_?)([]<>*#\n\t\r";

		 for (int i = 0; i < PUNCTUATION.length(); i++) {
			 System.out.println((int)PUNCTUATION.charAt(i)+"  "+PUNCTUATION.charAt(i));
			 System.out.println("----"+Character.getType(PUNCTUATION.charAt(i)));
		
			 
		}*/
		
		

		ClientUtil.initClient("dfyx", "10.27.70.135");
		
		QueryTest qt=new QueryTest();
		//qt.query1();
		qt.query2();
		
		
		ClientUtil.close();
		//QueryTest qt=new QueryTest();
		//qt.testAnalyzer();
		



/*ClientUtil.initClient(Initialize.esname, Initialize.eshost);

		QueryTest qt=new QueryTest();
		
		for (int i = 0; i < 1; i++) {
			qt.query();

		}

		
		//qt.testKryo();
		ClientUtil.close();*/
		
	}

}
