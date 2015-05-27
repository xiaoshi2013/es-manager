/*package com.zyuc.es.input.client;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.format.DateTimeFormat;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.zyuc.es.util.ClientUtil;
import com.zyuc.es.util.Format;
import com.zyuc.es.util.Pattern;
import com.zyuc.es.util.Utils;

public class ZKIndicesService implements IndicesService {

	private static final Logger _LOG = Logger.getLogger(ZKIndicesService.class);

	private CuratorFramework curator;

	private volatile CountDownLatch keepAliveLatch = new CountDownLatch(1);
	private volatile Thread keepAliveThread;

	private volatile Set<String> indices;
	
	private volatile boolean b=false; 

	private Client client;
	
	public Client getClient() {
		return client;
	}

	private static volatile ZKIndicesService singleton = null;
	
	public static  ZKIndicesService getInstance(String zkConnString,Client client) throws Exception{
		if (singleton == null) {
			synchronized (ZKIndicesService.class) {
				singleton=new ZKIndicesService(zkConnString, client);
			}
		}
		return singleton;
	}
	private ZKIndicesService(String zkConnString,Client client) throws Exception {
		this.curator = CuratorFrameworkFactory.builder()
				.connectString(zkConnString)
				.connectionTimeoutMs(30 * 1000)
				.sessionTimeoutMs(60 * 1000)
				.retryPolicy(new ExponentialBackoffRetry(1000, 120))
				.namespace(Utils.zknamespace).build();
		this.client=client;
		curator.start();
		curator.getZookeeperClient().blockUntilConnectedOrTimedOut();

		

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				try {
					keepAliveLatch.countDown();
					System.out.println("ThirdPartyCall.ThirdPartyCall(...).new Thread() {...}.run() "+keepAliveLatch.getCount());

				} catch (Exception e) {
					e.printStackTrace();
				}
				
			}
		});

		keepAliveThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					keepAliveLatch.await();
				} catch (InterruptedException e) {
					_LOG.error(e);
				}
			}
		}, "curator-zkcli");
		keepAliveThread.setDaemon(false);
		keepAliveThread.start();

		
		obtainIndices();
	}

	private void obtainIndices() throws Exception {

		final NodeCache node_cache = new NodeCache(this.curator, Utils.zklatestpath);

		node_cache.getListenable().addListener(new NodeCacheListener() {
			@Override
			public void nodeChanged() throws Exception {
				ChildData current_data = node_cache.getCurrentData();
				if (current_data == null) {
					_LOG.warn("zk no "+ Utils.zklatestpath +" data");
				} else {

					try {
						String[] ss1 = JSON.parseObject(current_data.getData(), String[].class);
						if (ss1 != null) {
							indices = Sets.newHashSet(ss1);

						}

						b=true;
					} catch (JSONException e) {
						_LOG.warn("wait " + e.getMessage());
					}
				}

			}
		});

		node_cache.start();

	}

	

	public String obtainLatestIndexName(String service) {
		return obtainLatestIndexName(service, true, null);
	}
	
	public String obtainLatestIndexName(String service,boolean isByDate) {
		return obtainLatestIndexName(service, isByDate, null);
	}
	
	public String obtainLatestIndexName(String service,boolean isByDate,Format format) {

		while(!b){
			_LOG.debug("wait...zk");
			try {
				TimeUnit.MILLISECONDS.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		_LOG.debug(ArrayUtils.toString(indices));
		if (this.indices != null) {
			for (String index : indices) {
				String prefix=Pattern.parsing(index);
				if (prefix.equals(service)) {
					return index;
				}
			}
			
				
			
		}
		// service是新的 之前没有 不用担心不一致
		if(isByDate){
			if(format==null){
				format=Format.format1;
			}
			
			service=StringUtils.removeEnd(service, "-");
			String index=service+"-"+DateTime.now().toString(DateTimeFormat.forPattern(format.getFormatter()));
			_LOG.info("---------index "+index);
			boolean b=client.admin().indices().exists(new IndicesExistsRequest(index)).actionGet().isExists();
			CreateIndexRequest request=null;
			if(!b){
				// 没有这个索引 是第一次创建 所以按照时间间隔选择起止时间 并加上LATEST
				request=new CreateIndexRequest(index);
				CreateIndexResponse res= client.admin().indices().create(request).actionGet();
				_LOG.debug(" create index "+index+ " "+ res.isAcknowledged());
				
			}
			indices.add(index);

			return index;
			
		}
		// 没有日期后缀
		
		boolean b=client.admin().indices().exists(new IndicesExistsRequest(service)).actionGet().isExists();
		if(!b){
			// 没有这个索引 是第一次创建  并加上LATEST
			CreateIndexRequest request=new CreateIndexRequest(service);
			CreateIndexResponse res= client.admin().indices().create(request).actionGet();
			_LOG.debug(" create index "+service+ " "+ res.isAcknowledged());
			
		}
		
		indices.add(service);
		return service;
	}

	public void close() {
		try {
			ClientUtil.close();
			this.curator.close();
			keepAliveLatch.countDown();
			_LOG.debug("ThirdPartyCall.close() "+keepAliveLatch.getCount() );
		} catch (Exception e) {
			e.printStackTrace();
		}
	
	}

}
*/