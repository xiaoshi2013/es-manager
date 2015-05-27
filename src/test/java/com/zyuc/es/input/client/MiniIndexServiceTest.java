package com.zyuc.es.input.client;

import java.util.concurrent.TimeUnit;

import com.zyuc.es.util.Format;


public class MiniIndexServiceTest {
	
	
	public static String[] getThreadNames() {
		ThreadGroup group = Thread.currentThread().getThreadGroup();
		ThreadGroup parent = null;
		while ((parent = group.getParent()) != null) {
			group = parent;
		}
		Thread[] threads = new Thread[group.activeCount()];
		group.enumerate(threads);
		java.util.HashSet set = new java.util.HashSet();
		for (int i = 0; i < threads.length; ++i) {
			if (threads[i] != null && threads[i].isAlive()) {
				try {
					System.out.println(threads[i].getThreadGroup().getName() + "," + threads[i].getName() + "," + threads[i].getPriority());
					
					 StackTraceElement[] st=  threads[i].getStackTrace();
					
					 for (StackTraceElement stackTraceElement : st) {
						System.out.println(stackTraceElement);
					}
					set.add(threads[i].getThreadGroup().getName() + "," + threads[i].getName() + "," + threads[i].getPriority());
				} catch (Throwable e) {
					e.printStackTrace();
				}
			}
		}
		String[] result = (String[]) set.toArray(new String[0]);
		java.util.Arrays.sort(result);
		return result;
	}

	
	public static void main(String[] args) throws Exception {

		//ClientUtil.initClient("product", "192.168.6.203");
		
		//ClientUtil.initClient("elasticsearch", "localhost");
		
	/*	IndicesStatsResponse  res=ClientUtil.getClient().admin().indices().prepareStats("ddos-2014.11").execute().actionGet();
		
		System.out.println(res.getIndex("ddos-2014.11").getPrimaries().store.getSizeInBytes());
		System.out.println(res.getIndex("ddos-2014.11").getPrimaries().docs.getCount());*/
		
		MiniIndexService service=MiniIndexService.getInstance();
		service.doStart();
		
		
		for (int i = 0; i < 1000; i++){
	
			System.out.println("----- " + service.obtainLatestIndexName("kafka", false));
			//System.out.println(service.obtainLatestIndexName("msg", false));
			//System.out.println(service.obtainLatestIndexName("msg", false,Format.format5));
			
			/*System.out.println(service.obtainLatestIndexName("cve", true));
			System.out.println(service.obtainLatestIndexName("cve2.0", true));
			System.out.println(service.obtainLatestIndexName("syslog", false));
			System.out.println(service.obtainLatestIndexName("flux", false));
			System.out.println(service.obtainLatestIndexName("user", false));
			System.out.println(service.obtainLatestIndexName("log", false));
			System.out.println(service.obtainLatestIndexName("ddos", false));
			System.out.println(service.obtainLatestIndexName("ddos_src_analysis", false));
			System.out.println(service.obtainLatestIndexName("ddos-status", false));
			System.out.println(service.obtainLatestIndexName("ddos_stats", false));
*/
			//System.out.println(service.latest);
			
			TimeUnit.SECONDS.sleep(3);
			
			
		}
		
		service.doStop();
	service.getNettyClientUtil().doClose();
		//getThreadNames();
	
	}
}
