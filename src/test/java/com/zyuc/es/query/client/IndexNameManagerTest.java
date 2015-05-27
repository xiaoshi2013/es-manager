package com.zyuc.es.query.client;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.index.query.QueryBuilders;

import com.zyuc.es.util.ClientUtil;
import com.zyuc.es.util.Format;
import com.zyuc.es.util.Initialize;

public class IndexNameManagerTest {
	
	public static void main(String[] args) {
		
		IndexNameManager manager=new IndexNameManager();
		ClientUtil.initClient(Initialize.esname, Initialize.eshost);
		long start=System.currentTimeMillis();

		System.out.println("index----- " + manager.obtainLatestIndexName("atstorm", false));
		System.out.println("index------"+manager.obtainLatestIndexName("msg", false));
		System.out.println("index------"+manager.obtainLatestIndexName("msg", false,Format.format5));
		System.out.println("index------"+manager.obtainLatestIndexName("msg", true));
		//System.out.println("index------"+manager.obtainLatestIndexName("zhidao", true));
		
		
		//while(true){
		/*System.out.println("---- "+Arrays.toString(manager.getIndexName("atstorm","2014.10.29")));
		System.out.println("---- "+Arrays.toString(manager.getIndexName("atstorm",3)));
		System.out.println("---- "+Arrays.toString(manager.getIndexName("atstorm", DateTime.now()
				.plusDays(1).getMillis(), DateTime.now().plusDays(2).getMillis())));*/
/*		System.out.println("---- "+Arrays.toString(manager.getIndexName("4a",90)));
		
		System.out.println("---- "+Arrays.toString(manager.getIndexNameAll("atstorm")));
		

		System.out.println("--------- "+Arrays.toString(manager.getIndexName("atstorm", 20)));
		System.out.println("--------- "+Arrays.toString(manager.getIndexName("atstorm", 1408723200000L,1417449599999L)));
		System.out.println("--------- "+Arrays.toString(manager.getIndexName("syslog",400)));*/
		
		
		/*SearchResponse res = ClientUtil.getClient()
				.prepareSearch(manager.getIndexName("atstorm","2014.10.29")).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
		
		System.out.println(res);
		
		System.out.println(System.currentTimeMillis()-start+" ms");
			try {
				TimeUnit.SECONDS.sleep(1);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}*/
			
		//}
			
	
	System.exit(0);		
	}

}
