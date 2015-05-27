package com.zyuc.es.query.client;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticsearch.common.joda.time.DateTime;

public class MiniIndexReadServiceTest {

	public static void main(String[] args) {
		//ClientUtil.initClient("product", "127.0.0.1");
		

		final MiniIndexReadService instance=MiniIndexReadService.getInstance();
		instance.doStart();
		
		final AtomicInteger atomic=new AtomicInteger();
		int n=100;
		for (int i = 0; i < n; i++) {
			Thread t=new Thread(new Runnable() {
				
				@Override
				public void run() {
					// TODO Auto-generated method stub
					/*instance.getIndexNameAll("atstorm");
					instance.getIndexName("atstorm",30);
					instance.getIndexName("atstorm","2015-03");*/
					
				//	System.out.println("1---- "+Arrays.toString(instance.getIndexName("atstorm","2014.10.29")));
					//System.out.println("2---- "+Arrays.toString(instance.getIndexName("atstorm",3)));

					System.out.println("3---- "+Arrays.toString(instance.getIndexName("atstorm", 
							DateTime.now().plusDays(1).getMillis(), DateTime.now().plusDays(2).getMillis())));
					
					
					atomic.incrementAndGet();
					//instance.getIndexName("atstorm", 1408723200000L,1417449599999L);

				}
			});
			t.start();
			
			try {
				TimeUnit.SECONDS.sleep(3);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		

		}
		
		while(atomic.get()!=n){
			try {
				TimeUnit.MILLISECONDS.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
		}
		instance.doStop();

		System.exit(0);
	
		
	/*	while(true){
		//System.out.println("----------"+Arrays.toString(	instance.getIndexName("atstorm",100)));
		System.out.println("---- "+Arrays.toString(instance.getIndexName("atstorm","2014.10.29")));
		System.out.println("---- "+Arrays.toString(instance.getIndexNameAll("atstorm")));
		System.out.println("---- "+Arrays.toString(instance.getIndexNameAll("cve")));

		System.out.println("--------- "+Arrays.toString(instance.getIndexName("atstorm", 20)));
		System.out.println("--------- "+Arrays.toString(instance.getIndexName("atstorm", 1408723200000L,1417449599999L)));
		
		
		
		System.out.println(System.currentTimeMillis()-start+" ms");
			try {
				TimeUnit.SECONDS.sleep(1);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}*/
		
	}
}
