/*package com.zyuc.es.query.client;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import org.elasticsearch.common.joda.time.DateTime;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.google.common.collect.Sets;
import com.zyuc.es.util.Utils;

public class QueryClient {

	private static final Logger _LOG = Logger.getLogger(QueryClient.class);

	private CuratorFramework curator;

	private volatile CountDownLatch keepAliveLatch;
	private volatile Thread keepAliveThread;

	private volatile Map<String, Collection<Long>> indices_date;

	private volatile Map<String, Collection<String>> service_indices;

	private volatile boolean b = false;
	private volatile boolean b1 = false;

	public QueryClient(String zkConnString) throws Exception {
		this.curator = CuratorFrameworkFactory.builder().connectString(zkConnString).connectionTimeoutMs(30 * 1000).sessionTimeoutMs(60 * 1000 * 2).retryPolicy(new ExponentialBackoffRetry(1000, 120))
				.namespace(Utils.zknamespace).build();

		curator.start();
		curator.getZookeeperClient().blockUntilConnectedOrTimedOut();

		keepAliveLatch = new CountDownLatch(1);

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				keepAliveLatch.countDown();
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
		}, "QueryClient-zkcli");
		keepAliveThread.setDaemon(false);
		keepAliveThread.start();

		obtainServiceIndices();
		obtainIndices();
	}

	private void obtainIndices() throws Exception {

		final NodeCache node_cache = new NodeCache(this.curator, Utils.zkdatepath);

		node_cache.getListenable().addListener(new NodeCacheListener() {
			@Override
			public void nodeChanged() throws Exception {
				ChildData current_data = node_cache.getCurrentData();
				if (current_data == null) {
					_LOG.warn("zk 没有数据");
				} else {



					try {
						Map<String, Collection<Long>> ss1 = JSON.parseObject(current_data.getData(), Map.class);
						if (ss1 != null) {
							indices_date = ss1;

						}

						b = true;
					} catch (JSONException e) {
						_LOG.warn("wait " + e.getMessage());
					}
				}

			}
		});

		node_cache.start();

	}

	private void obtainServiceIndices() throws Exception {

		final NodeCache node_cache = new NodeCache(this.curator, Utils.zkpath);

		node_cache.getListenable().addListener(new NodeCacheListener() {
			@Override
			public void nodeChanged() throws Exception {
				ChildData current_data = node_cache.getCurrentData();
				if (current_data == null) {
					_LOG.warn("zk 没有数据");
				} else {
					// System.out.println("Data change watched, and current data = "
					// + new String(current_data.getData()));

					try {
						Map<String, Collection<String>> ss1 = JSON.parseObject(current_data.getData(), Map.class);
						if (ss1 != null) {
							service_indices = ss1;

						}

						b1 = true;
					} catch (JSONException e) {
						_LOG.warn("wait " + e.getMessage());
					}
				}

			}
		});

		node_cache.start();

	}

	*//**
	 * 指定索引名称
	 * 
	 * @return 唯一索引名
	 *//*
	public String specifyIndexName() {
		while (!b) {
			_LOG.info("wait...zk");
			try {
				TimeUnit.MILLISECONDS.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		return null;

	}

	public String[] getIndexNames(String service, long from, long to) {
		Set<String> indexName = Sets.newLinkedHashSet();
		while (!b) {
			_LOG.info("wait...zk");
			try {
				TimeUnit.MILLISECONDS.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		service = StringUtils.removeEnd(service, "-");

		DateTime dtfrom = new DateTime(from);
		from = dtfrom.toLocalDate().toDateTimeAtStartOfDay().getMillis();
		if (to <= 0 || to >= DateTime.now().getMillis()) {
			to = DateTime.now().getMillis();
		}

		DateTime dtto = new DateTime(to);
		to = dtto.toLocalDate().toDateTimeAtStartOfDay().getMillis();

		_LOG.debug(dtfrom.toLocalDateTime() + " " + dtto.toLocalDateTime());


		_LOG.debug(indices_date);
		if (this.indices_date != null) {
			Map<String, Collection<Long>> indices_date_ = indices_date;

			// System.out.println(indices_date_.keySet());

			Collection<String> indices = service_indices.get(service);

			// System.out.println("--- "+indices);
			if (indices == null) {
				return null;
			}
			for (String index : indices) {
				Collection<Long> coll = indices_date_.get(index);
				// System.out.println(index+" "+coll);
				if (coll == null || coll.size() == 0) {
					continue;
				}
				Long[] times = coll.toArray(new Long[coll.size()]);

				for (Long long1 : times) {
					if (long1 >= from && long1 <= to) {
						indexName.add(index);
					}
				}

			}

		}

		return indexName.toArray(new String[indexName.size()]);
	}

	
	*//**
	 * 获得服务下全部索引
	 * @param service
	 * @return
	 *//*
	public String[] getIndexNames(String service) {
		Set<String> indexName = Sets.newLinkedHashSet();
		while (!b1) {
			_LOG.info("wait...zk service_indices");
			try {
				TimeUnit.MILLISECONDS.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		service = StringUtils.removeEnd(service, "-");

		Collection<String> indices = service_indices.get(service);

		indexName.addAll(indices);

		return indexName.toArray(new String[indexName.size()]);
	}

	public void close() {
		this.curator.close();
	}

}
*/