package com.zyuc.es.server;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.ArrayListMultimap;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.collect.Multimap;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.LocalDate;
import org.elasticsearch.common.joda.time.format.DateTimeFormat;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import com.esotericsoftware.minlog.Log;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.zyuc.es.util.ClientUtil;
import com.zyuc.es.util.Format;
import com.zyuc.es.util.Initialize;
import com.zyuc.es.util.Pattern;
import com.zyuc.es.util.Tuple;
import com.zyuc.es.util.Utils;

/**
 * 获取入库时的最新索引名称的服务端模块
 * @author zhanglei
 *
 */
public class LatestIndicesServiceThread extends ServicesAndIndices implements Runnable {
	
	private static final Logger _LOG = Logger.getLogger(LatestIndicesServiceThread.class);

	 private  volatile Set<String>  services;

	 private	 volatile Map<String,String> latest;
	
	 private	 volatile Map<String,IndexStats> index_stats;

	 private	 volatile Multimap<String, String> service_index;
	 
	private final Map<String, Long> index_docs=Maps.newHashMap();

		
	 static final Map<String, Long> indexLasttime = Maps.newLinkedHashMap();

	private AtomicLong invocations=new AtomicLong(0);

		
	ScheduledExecutorService scheduled;
	
	public LatestIndicesServiceThread( ScheduledExecutorService scheduled){
		
		this.scheduled=scheduled;
	}
	
	@Override
	public void run() {
		try {
			if(stop){
				System.out.println("LatestIndicesServiceThread.run() stop = "+stop);
				return;
			}
			long start=System.currentTimeMillis();
			String str=obtainIndices();
			//_LOG.debug(str+"------------- "+  (System.currentTimeMillis()-start)+" ms");
			//_LOG.debug(latest);
			
			scheduled.schedule(this, Initialize.write_interval, TimeUnit.SECONDS);
		} catch (Exception e) {
			_LOG.error("zhangl error: "+e.getMessage());
		}
	}

	
	private String obtainIndices() throws Exception {
		if (invocations.get() == 0) {
			Tuple tuple = Utils.indexServerDeserialization();
			if (tuple != null) {
				Map<String, Long> indexLasttime_ = (Map<String, Long>) tuple.v2();
				Map<String, Long> index_doc = (Map<String, Long>) tuple.v1();

				if (indexLasttime_ != null && indexLasttime_.size() > 0) {
					indexLasttime.putAll(indexLasttime_);
				}
				if (index_doc != null && index_doc.size() > 0) {
					index_docs.putAll(index_doc);
				}

			}

		}
		invocations.incrementAndGet();
		
		Client client= ClientUtil.getClient();
		//_LOG.debug(((TransportClient)client).connectedNodes());
		IndicesStatsResponse res = client.admin().indices().prepareStats().execute().actionGet();
		Map<String, IndexStats> map = res.getIndices(); // 所有索引名称
		index_stats=map;
		
		//_LOG.debug("-------------------- index_stats:");
		//_LOG.debug(index_stats.keySet());
		
		Multimap<String, String> service_index_ =ArrayListMultimap.create();
		Set<String> services_=Sets.newHashSet();
		Map<String,String> latest_ = Maps.newConcurrentMap();
		
		
		if(index_stats==null || index_stats.size()==0){
			_LOG.info("index_stats is null... ");
			latest=latest_;
			services=services_;
			service_index=service_index_;
			return EMPTY;
		}
		
		
		Set<String> set = map.keySet();
	
		for (String indexName : set) {
			String service=Pattern.parsing(indexName); // 没日期的索引 只有一个 默认最新 service 不带_
			services_.add(service);
			if(service.equals(indexName)){
				//System.out.println("only one--- "+indexName);
				//latest_.put(service, indexName);
				Pattern.no_timetamp_indices.add(indexName);
				continue;
			}
			service_index_.put(service, indexName);
		
		}
		
		services=services_;
		service_index=service_index_;
		if(services==null || services.size()==0){
			latest=latest_;
			_LOG.info("No data services... ");
			return EMPTY;
		}
		
		for (String service : service_index_.keySet()) {
			Collection<String>  coll=service_index_.get(service);
			//System.out.println("LatestIndicesServiceThread.obtainIndices()---- "+coll);
			
			try {
				String index=queryTimestampSort(coll.toArray(new String[coll.size()]),map, Utils.TIMESTAMP,service);
				
				if(!StringUtils.isBlank(index) && !index.equals(service)){
					
					latest_.put(service, index);
				}
			} catch (Exception e) {
				_LOG.error(service+ "-------error------- ",e);
				continue;
			}
		
		
		}
		
		
		latest=latest_;
		
		Utils.indexServerSerialization(indexLasttime,index_docs);
		
		
	/*	if(latest==null || latest.size()==0){
			_LOG.info("No data latest... ");
			return EMPTY;
		}*/
		
		
		return SUCCESS;
		
		
	}
	
	String queryTimestampSort(String[] indices,Map<String,IndexStats> statsMap,String field, String service){

		//System.out.println("LatestIndicesServiceThread.queryTimestampSort() "+service+"   "+Arrays.toString(indices));
		long t=LocalDate.now().minusDays(1).toDate().getTime();
		DateTime dt=Pattern.dayTime();
		if(indices==null|| indices.length==0){
			return null;
		}
		
		// 直接返回
		if(indices.length==1){
			return indices[0];
		}
	
		// 取序号最大的增量
		/*String name=Pattern.determineIncremental(indices, service);

		
		if(!StringUtils.isBlank(name)){
			return name;
		}*/
		

		
		//_LOG.debug("The following should not come********************");
		
		//剩下都是有日期的 
		Map<String, Long> indxlasttime_ = Maps.newLinkedHashMap();
		
		for (int i = 0; i < indices.length; i++) {
			String index=indices[i];
			long docs=statsMap.get(index).getTotal().getDocs().getCount();
			
			if(Pattern.no_timetamp_indices.contains(index)){
				return index;
				
			}
			if(docs==0){
				index_docs.put(index, docs);
				return index;
			}
			
			
			if(index_docs.get(index)==null || index_docs.get(index).longValue()!=docs){
				index_docs.put(index, docs);
			}
		
			else if(index_docs.get(index).longValue()==docs && indexLasttime.get(index)!=null){// doc没变的之前已经查询过时间 不用再查了
				indxlasttime_.put(index,indexLasttime.get(index));
				continue;
			}
		
			
			// 之前已经查询过时间的索引 但是docs变了 再查一遍
			if(indexLasttime.get(index)!=null){
				indxlasttime_.put(index,indexLasttime.get(index));
			}
		
			// 如果是新增的索引 或者是数量有变化的索引  查询最新的@timestamp
				SearchRequestBuilder search=new SearchRequestBuilder(ClientUtil.getClient());
				search.setIndices(index);
				search.setQuery(QueryBuilders.rangeQuery("@timestamp").from(0).to(dt.getMillis()));
				search.setSize(1);
				search.setRouting("4");
				search.addSort(SortBuilders.fieldSort("@timestamp").order(SortOrder.DESC));
			
				try {
					SearchResponse  res=search.execute().actionGet();
					_LOG.info("---- "+index+" docs "+docs+" took "+res.getTook());
					long val=(long) res.getHits().getHits()[0].getSortValues()[0];
					
					indexLasttime.put(index,val);
					indxlasttime_.put(index,val);
					//System.out.println( val +" "+indices[i]);
					
				} catch (Exception e) {
					_LOG.error("Discarded "+index);
					Pattern.no_timetamp_indices.add(index);
					indexLasttime.remove(index);
					indxlasttime_.remove(index);
					//IndicesUpdateScheduler.indxlasttime.put(indices[i],dt.getMillis());
					
				}
			
			
		}
		
		
		String latestName="";
		if(indxlasttime_.size()==0){
			
			return latestName;
		}
		
		List<Map.Entry<String, Long>> coll= Lists.newArrayList(indxlasttime_.entrySet());
	  Collections.sort(coll, new Comparator<Map.Entry<String, Long>>() {

		@Override
		public int compare(Entry<String, Long> o1, Entry<String, Long> o2) {
			  return o1.getValue().compareTo(o2.getValue());  
			
		}
	});
      


		latestName=coll.get(coll.size()-1).getKey();
		
	
		return latestName;
	}
	
	
	public String generateNewIndex(String service,Collection<String> coll){
		
			Format format=Format.format1;
			
			String index_=service+"-"+DateTime.now().toString(DateTimeFormat.forPattern(format.getFormatter()));
			_LOG.info("---------new index: "+index_);
			boolean b=coll.contains(index_);
			CreateIndexRequest request=null;
			if( !b){
				// 没有这个索引 是第一次创建 所以按照时间间隔选择起止时间 并加上LATEST
				request=new CreateIndexRequest(index_);
				CreateIndexResponse res= ClientUtil.getClient() .admin().indices().create(request).actionGet();
				_LOG.debug(" create index "+index_+ " "+ res.isAcknowledged());
				
			}
			latest.put(service, index_);
			
			return index_;
	
		
	}

	public Set<String> getServices() {
		return services;
	}

	public void setServices(Set<String> services) {
		this.services = services;
	}

	public Map<String, String> getLatest() {
		return latest;
	}

	public void setLatest(Map<String, String> latest) {
		this.latest = latest;
	}

	public Map<String, IndexStats> getIndex_stats() {
		return index_stats;
	}

	public void setIndex_stats(Map<String, IndexStats> index_stats) {
		this.index_stats = index_stats;
	}

	public Multimap<String, String> getService_index() {
		return service_index;
	}

	public void setService_index(Multimap<String, String> service_index) {
		this.service_index = service_index;
	}
	
}
