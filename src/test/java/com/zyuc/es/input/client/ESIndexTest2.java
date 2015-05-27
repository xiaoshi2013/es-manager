package com.zyuc.es.input.client;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.joda.time.DateTime;

import com.zyuc.es.util.ClientUtil;

public class ESIndexTest2 {

	// final static Logger logger = Logger.getLogger(ESIndexTest1.class);


	private int size;
	private int permits;
	BulkRequest bulkRequest = new BulkRequest();

	private final Semaphore semaphore;

	private MiniIndexService service;

	private volatile boolean finish;
	/**
	 * @param name
	 * @param host
	 * @param size
	 * @param permits
	 * @param port
	 * @throws IOException
	 */
	public ESIndexTest2(int size, int permits, int port) throws IOException {

		this.size = size;
		this.permits = permits;
		semaphore = new Semaphore(permits);

		try {

			service=(MiniIndexService) IndicesServiceFactory.createIndicesService();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	public void testIndex(int times) throws IOException {



		File f = new File("test");
		final List<String> lines = Files.readAllLines(Paths.get(f.toURI()), Charset.forName("UTF-8"));
		final int len = lines.size();

		// ExecutorService exe=Executors.newFixedThreadPool(4);

		// final CountDownLatch down=new CountDownLatch(4);

		Long start = System.currentTimeMillis();

		for (int l = 0; l < times; l++) {
			try {
				
				String index = service.obtainLatestIndexName("msg", false);
				//System.out.println("ESIndexTest2.testIndex() index " + index);

				
				for (int i = 0; i < len; i++) {

					String msg = lines.get(i);
					Map map = Maps.newHashMap();

					map.put("message", msg);
					map.put("response", Integer.valueOf(StringUtils.split(msg)[1]));
					map.put("srcip", StringUtils.split(msg)[2]);
					String time = StringUtils.substring(msg, 0, 14);
					time = time.replaceAll("\\.", "");
					DateTime dt = new DateTime(Long.valueOf(time));
					dt = dt.minusDays(ThreadLocalRandom.current().nextInt(10));
					dt = dt.withHourOfDay(ThreadLocalRandom.current().nextInt(23));
					dt = dt.withMinuteOfHour(ThreadLocalRandom.current().nextInt(59));
					dt = dt.withSecondOfMinute(ThreadLocalRandom.current().nextInt(59));
					map.put("timestamp", dt.getMillis());

					map.put("@timestamp", dt);
					map.put("status_code", StringUtils.split(msg)[3].split("/")[0]);
					map.put("status", StringUtils.split(msg)[3].split("/")[1]);
					map.put("size", Integer.valueOf(StringUtils.split(msg)[4]));
					map.put("method", StringUtils.split(msg)[5]);

					//bulkRequest.add(new IndexRequest(index, "test").source(map).replicationType(ReplicationType.ASYNC).consistencyLevel(WriteConsistencyLevel.ONE));
					
					bulkRequest.add(new IndexRequest(index, "test").source(map));

					executeIfNeeded();

				}

			} catch (Exception e) {
				e.printStackTrace();
			}

		}

		if (bulkRequest.numberOfActions() > 0) {
			long start1 = System.currentTimeMillis();
			BulkResponse res = ClientUtil.getClient().bulk(bulkRequest).actionGet();

			System.out.println("last time " + (System.currentTimeMillis() - start1) + " " + res.getTook() + " " + res.getItems().length);
		}

		int available = semaphore.availablePermits();

		System.out.println("semaphore " + semaphore + " " + available);
		while (available < permits) {
			try {
				available = semaphore.availablePermits();
				// System.out.println(available);
				TimeUnit.MILLISECONDS.sleep(300);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		// System.out.println("before commit "+(System.currentTimeMillis()-start)+" ms");
		System.out.println("after commit " + (System.currentTimeMillis() - start) + " ms");
		String index = service.obtainLatestIndexName("msg", false);
		IndicesStatsResponse res = ClientUtil.getClient().admin().indices().prepareStats(index).execute().actionGet();
		// FlushResponse
		// flushres=client.admin().indices().prepareFlush(index).execute().actionGet();

		ClientUtil.getClient().admin().indices().prepareRefresh(index).execute().actionGet();

		finish=true;
		System.out.println("docs " + res.getPrimaries().docs.getCount());

	}

	private void executeIfNeeded() {
		if (bulkRequest.numberOfActions() < size) {
			return;
		}

		BulkRequest bulk = bulkRequest;

		bulkRequest = new BulkRequest();

		final long start = System.currentTimeMillis();
		boolean success = false;
		try {
			semaphore.acquire();
			ClientUtil.getClient().bulk(bulk.replicationType(ReplicationType.ASYNC).consistencyLevel(WriteConsistencyLevel.ONE), new ActionListener<BulkResponse>() {

				@Override
				public void onResponse(BulkResponse res) {

					//System.out.println(Thread.currentThread().getName() + " " + res.getTookInMillis()+" "+res.getItems().length);

					semaphore.release();

				}

				@Override
				public void onFailure(Throwable e) {
					e.printStackTrace();

				}
			});

			success = true;
		} catch (InterruptedException e1) {
			e1.printStackTrace();
			if (!success) {
				semaphore.release();
			}
		}

	}

	public static void main(String[] args) throws Exception {

		ClientUtil.initClient("es-monitor", "localhost");

		ESIndexTest2 test = new ESIndexTest2( 1000, 1, 9300);

		for (int i = 0; i < 1; i++) {
			test.testIndex(1000);
			//TimeUnit.SECONDS.sleep(5);

		}
		
		
			//test.service.close();

		


	}
}
