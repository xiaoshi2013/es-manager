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
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.index.query.QueryBuilders;

import com.zyuc.es.util.BulkProcessorTool;
import com.zyuc.es.util.ClientUtil;
import com.zyuc.es.util.Format;
import com.zyuc.es.util.Initialize;

public class ESIndexTest2ByBulkProcessTool {

	// final static Logger logger = Logger.getLogger(ESIndexTest1.class);


	private int size;
	//private int permits;

	//private final Semaphore semaphore;

	private MiniIndexService service;

	/**
	 * @param name
	 * @param host
	 * @param size
	 * @param permits
	 * @param port
	 * @throws IOException
	 */
	public ESIndexTest2ByBulkProcessTool(int size, int permits, int port) throws IOException {

		this.size = size;


		try {
			ClientUtil.initClient(Initialize.esname, Initialize.eshost);


			service=(MiniIndexService) IndicesServiceFactory.createIndicesService();
			BulkProcessorTool.init(ClientUtil.getClient());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	public void testIndex(int times) throws IOException {



		File f = new File("test");
		final List<String> lines = Files.readAllLines(Paths.get(f.toURI()), Charset.forName("UTF-8"));
		final int len = lines.size();


		
		Long start = System.currentTimeMillis();
		String index = service.obtainLatestIndexName("kafka", false);

		for (int l = 0; l < times; l++) {
			try {
				
				
				//System.out.println("------ "+index);
				//String index = service.obtainLatestIndexName("msg", true);
				
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
					dt = dt.withYear(2015);
					dt = dt.withMonthOfYear(ThreadLocalRandom.current().nextInt(1,12));
					dt = dt.minusDays(ThreadLocalRandom.current().nextInt(200));
					dt = dt.withHourOfDay(ThreadLocalRandom.current().nextInt(23));
					dt = dt.withMinuteOfHour(ThreadLocalRandom.current().nextInt(59));
					dt = dt.withSecondOfMinute(ThreadLocalRandom.current().nextInt(59));
					//map.put("timestamp", dt.getMillis());


					map.put("@timestamp", dt);
					DateTime dtc= new DateTime();
					//System.out.println(dtc);
					map.put("@timestamp_c",dtc);
					
					map.put("status_code", StringUtils.split(msg)[3].split("/")[0]);
					map.put("status", StringUtils.split(msg)[3].split("/")[1]);
					map.put("size", Integer.valueOf(StringUtils.split(msg)[4]));
					map.put("method", StringUtils.split(msg)[5]);

					//bulkRequest.add(new IndexRequest(index, "test").source(map).replicationType(ReplicationType.ASYNC).consistencyLevel(WriteConsistencyLevel.ONE));

					
					BulkProcessorTool.add(new IndexRequest(index,"test").source(map));


				}

			} catch (Exception e) {
				e.printStackTrace();
			}

		}

		BulkProcessorTool.flush();
		System.out.println("Set "+BulkProcessorTool.atomicCounter());
		while(BulkProcessorTool.atomicCounter()>0){
			try {
				System.out.println("atomMap  "+BulkProcessorTool.atomicCounter());
				TimeUnit.MILLISECONDS.sleep(200);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	
		System.out.println("after commit " + (System.currentTimeMillis() - start) + " ms");
		/*String index = service.obtainLatestIndexName("msg", false);
		
		ClientUtil.getClient().admin().indices().prepareRefresh().execute().actionGet();
		
		IndicesStatsResponse res = ClientUtil.getClient().admin().indices().prepareStats(index).execute().actionGet();
		System.out.println("getIndexCount" + res.getPrimaries().indexing.getTotal().getIndexCount());*/
		
	//SearchResponse sres=	service.getClient().prepareSearch("msg-2015*").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
		
	//System.out.println(sres);
		service.doStop();
		ClientUtil.close();
		service.getNettyClientUtil().doClose();

	}



	public static void main(String[] args) throws Exception {

		//ClientUtil.initClient("test", "192.168.6.207");

		
		ESIndexTest2ByBulkProcessTool test = new ESIndexTest2ByBulkProcessTool( 1000, 1, 9300);

		for (int i = 0; i < 1; i++) {
			test.testIndex(1);
			//TimeUnit.SECONDS.sleep(5);

		}
		
		
		

		


	}
}
