package com.zyuc.es.util;

import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.unit.TimeValue;

import com.zyuc.es.analyzer.MyUrlAnalyzer;

public class BulkProcessorTool {
	
	private static Logger _LOG = Logger.getLogger(BulkProcessorTool.class);

	static TimeValue flushInterval = TimeValue.timeValueSeconds(5);
	static int concurrentRequests =1;
	static int bulkActions=1000;
	
	private static final byte[] lock=new byte[0];
	


	private static volatile BulkProcessor processor;
	
	private static  Analyzer analyzer;
	
	static {
		//init();
	}
	/**
	 * 加一些公共字段
	 * @param indexRequest
	 * @return
	 */
	public static IndexRequest encapsulateFields(IndexRequest indexRequest)throws Exception{
		
		try {
			Map<String,Object> map1=indexRequest.sourceAsMap();
			Object obj = map1.get("@timestamp");
			
		/*	Object msg=map1.get("@message");
			String key="@message";
			if(msg==null){
				msg=map1.get("message");
				key="message";
			}*/
			
			/*if(msg!=null){
				String msgstr=msg.toString();
				TokenStream stream=	null;
				List terms=Lists.newArrayList(32);
				try {
					 stream=analyzer.tokenStream("msg", msgstr);
				
					 stream.reset();
					while(stream.incrementToken()){
						CharTermAttribute att = stream.getAttribute(CharTermAttribute.class);
						//System.out.println("term:"+att);
						terms.add(att);
					}
					  stream.end();
				} catch (IOException e) {
					e.printStackTrace();
					_LOG.error("error: "+e.getMessage());
				}
				finally {
		            if (stream != null) {
		                try {
		                    stream.close();
		                } catch (IOException e) {
		                    // ignore
		                }
		            }
				}
				
				//System.out.println( StringUtils.join(terms,' '));
				map1.put(key, StringUtils.join(terms,' '));
			}*/
			
			if(obj==null){
				obj=new DateTime();
			}
			DateTime dt=new DateTime(obj);
			int d=Integer.parseInt(dt.toLocalDate().toString("YYYYMMdd"));
			
			map1.put("@timestamp", dt);
			map1.put("@timestamp_y", dt.getYearOfEra());
			
			map1.put("@timestamp_d", d);
			map1.put("@timestamp_h", dt.getHourOfDay());
			
		
			
			indexRequest=indexRequest.source(map1);
		} catch (Exception e) {
				_LOG.error("error-----"+e);
				throw e;
		}

		_LOG.info("zhanglei index---------"+indexRequest.index());
		return indexRequest;
	
	}
	
	/*public static void init(){
		init(ClientUtil.getClient());
		
	}*/
	/**
	 * Initialized global singleton
	 * @param client  elasticsearch client  需要外部初始化后传入
	 */
	public static void init(Client client){
		if(processor==null){
			synchronized (lock) {
				if(processor==null){
					 processor = BulkProcessor.builder(client, new BulkListener())
						.setConcurrentRequests(concurrentRequests)
						.setBulkActions(bulkActions)
						.setFlushInterval(flushInterval)
						.build();
				}
				
				if(analyzer==null){
					CharArraySet set=new CharArraySet(Version.LUCENE_CURRENT,16,true);
					set.add('-');
					analyzer=new MyUrlAnalyzer(Version.LUCENE_CURRENT, set);
				}
				
			}

		}
		
		
	}
	
	
	public static void add(IndexRequest indexRequest)throws Exception {
		//_LOG.info("add------start");
		if(indexRequest!=null){
			try {
				_LOG.trace("index-------- "+indexRequest.index());
				processor.add(encapsulateFields(indexRequest)
						//.replicationType(ReplicationType.ASYNC)
						//.consistencyLevel(WriteConsistencyLevel.ONE)
						
						);
				
				//processor.add(indexRequest);
				//_LOG.info("add------over");

			} catch (Exception e) {
				_LOG.error("bulkerror----"+e.getMessage());
				
				throw e;
			}
		
		}
	
	
	}
	
	public static void flush(){
		//_LOG.info("flush-----");
		processor.flush();
		_LOG.info("flush----- over");
	}
	
	public static void close(){
		processor.close();
	}
	
	
	public static int atomicCounter(){
		return BulkListener.atomMap.size();
	}

	
}
