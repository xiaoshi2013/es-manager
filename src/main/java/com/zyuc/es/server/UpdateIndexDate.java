package com.zyuc.es.server;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram.Bucket;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram.Interval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Order;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.zyuc.es.util.ClientUtil;
import com.zyuc.es.util.Initialize;
import com.zyuc.es.util.Pattern;
import com.zyuc.es.util.Tuple;
import com.zyuc.es.util.Utils;

/**
 * 查询索引名称 服务端模块
 * @author zhanglei
 *
 */
public class UpdateIndexDate implements Runnable {
	
	private static final Logger LOG = Logger.getLogger(UpdateIndexDate.class);


	private volatile Set<String>  services=null;
	
	private volatile HashMultimap<String, String> service_indices;

	private volatile HashMultimap<String, Long> index_date;	
	
	private final Map<String, Long> index_docs=Maps.newHashMap();

	private static final ReentrantLock lock=new ReentrantLock();
	
	private AtomicLong invocations=new AtomicLong(0);

	static final int BASELINE=2010;
	
	 ScheduledExecutorService scheduled;
	 
	public UpdateIndexDate(ScheduledExecutorService scheduled) {
		this.scheduled=scheduled;
	}

	@Override
	public void run() {
		long start=System.currentTimeMillis();
		try {
			updateIndicesDate();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		LOG.debug("update indices "+ (System.currentTimeMillis()-start)+" ms");
		scheduled.schedule(this, Initialize.indexdate_interval, TimeUnit.SECONDS);

	}

	public void updateIndicesDate() throws Exception{

		if (invocations.get() == 0 && index_date == null) {
			Tuple tuple = Utils.readServerDeserialization();
			if (tuple != null) {
				Map<String, Collection<Long>> map = (Map<String, Collection<Long>>) tuple.v2();
				Map<String, Long> index_doc = (Map<String, Long>) tuple.v1();

				if (map != null) {
					index_date = HashMultimap.create();
					for (Map.Entry<String, Collection<Long>> entry : map.entrySet()) {
						index_date.putAll(entry.getKey(), entry.getValue());
					}
				}
				if (index_doc != null && index_doc.size() > 0) {
					index_docs.putAll(index_doc);
				}
			}

		}
		invocations.incrementAndGet();

		long start = System.currentTimeMillis();
		IndicesStatsResponse res = ClientUtil.getClient().admin().indices().prepareStats().execute().actionGet();
		Map<String, IndexStats> status = res.getIndices();
		Set<String> indices_ = status.keySet();
		Collection<IndexStats> collection = status.values();

		HashMultimap<String, String> service_indices_ = HashMultimap.create();

		Set<String> services_ = Sets.newHashSet();
			
			for (IndexStats indexStats : collection) {
				try {
					/*long translogId=indexStatus.getShards().get(0).getShards()[0].getTranslogId();
					
					SearchResponse sres=client.prepareSearch(indexStatus.getIndex())
							.setSize(1).addSort("@timestamp", SortOrder.DESC)
							.setPreference("_local").setSearchType(SearchType.DEFAULT)
							.setRouting("4")
					.execute().actionGet();
					
					DateTime d=new DateTime( sres.getHits().getHits()[0].sourceAsMap().get("@timestamp"));
					
					if(d.getMillis() < translogId){
						translogId= d.getMillis();
					}*/
				

					//index_translog.put(indexStatus.getIndex(), translogId);
					
					String service=Pattern.parsing(indexStats.getIndex()); // 没日期的索引默认最新 service 不带_
					services_.add(service);
					service_indices_.put(service, indexStats.getIndex());
				} catch (Exception e) {
					LOG.error("error no @timestamp index: "+indexStats.getIndex()+" "+e.getMessage());
				}
	
			}
			
			service_indices=service_indices_;
			services=services_;
			//LOG.debug("services "+ services);
			 HashMultimap<String, Long> index_date_=HashMultimap.create();
		    DateTime now=new DateTime();
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
						.setRouting("4")
						.addAggregation(AggregationBuilders.dateHistogram("day")
								.interval(Interval.DAY).field("@timestamp")
								.preZone("Asia/Shanghai")
								.preZoneAdjustLargeInterval(true)
								.order(Order.KEY_ASC))
								.execute().actionGet();
						
						LOG.debug(index +" took "+ searchres.getTook());
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
			
			
			try {
				lock.lock();
				if(index_date==null){
					index_date=index_date_;
				}
				else{
					if(invocations.get()>1){
						Set<String> cacheSet= index_date.keySet();
						Set<String> set1=Sets.newHashSet();
						for (String str : cacheSet) {
							if(!indices_.contains(str)){
								LOG.debug("not exist remove "+str);
								set1.add(str);
							}
						}
						
						cacheSet.removeAll(set1);
					}
					index_date_.putAll(index_date);
					index_date=index_date_;
					
					long st1=(System.currentTimeMillis());
					LOG.debug("----- get index_date Consuming "+(st1-start)+" ms");
					
					Utils.readServerSerialization(index_date_ ,index_docs);
					
				}
			} catch (Exception e) {
				LOG.error("error", e);
			}
			finally{
				lock.unlock();
			}
			
		
	}
	

	public Set<String> getServices() {
		return services;
	}

	public HashMultimap<String, String> getService_indices() {
		return service_indices;
	}

	public HashMultimap<String, Long> getIndex_date() {
		return index_date;
	}

	public Map<String, Long> getIndex_docs() {
		return index_docs;
	}

	public AtomicLong getInvocations() {
		return invocations;
	}

	
} 
