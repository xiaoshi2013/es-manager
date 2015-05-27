package com.zyuc.es.query.client;

import org.elasticsearch.index.fielddata.RamUsage;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache.Key;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

public class CacheTest {

	public void testCache(){
	   /*  CacheBuilder<Key, RamUsage> cacheBuilder=  CacheBuilder.newBuilder().removalListener(new RemovalListener<K, V>() {

			@Override
			public void onRemoval(RemovalNotification<K, V> notification) {
				// TODO Auto-generated method stub
				
			}
		});*/
	}
	
}
