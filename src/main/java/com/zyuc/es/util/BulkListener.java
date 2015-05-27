package com.zyuc.es.util;

import java.util.Arrays;
import java.util.Map;

import org.apache.log4j.Logger;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor.Listener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.common.collect.Maps;

public class BulkListener implements Listener {
	
	private static Logger _LOG = Logger.getLogger(BulkListener.class);

	public static final Map<Long,Long> atomMap=Maps.newConcurrentMap();



	
	@Override
	public void beforeBulk(long executionId, BulkRequest request) {
		_LOG.info("beforeBulk--------"+request.requests().size()+" executionId "+executionId);
		//_LOG.info("beforeBulk--------"+request.requests());

		
	//	request.replicationType(ReplicationType.ASYNC).consistencyLevel(WriteConsistencyLevel.ONE);
		 atomMap.put(executionId,System.currentTimeMillis());
	}
	
	@Override
	public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
		_LOG.error("bulkerror-----",failure);
		//failure.printStackTrace();
		atomMap.remove(executionId);
		
	}
	
	@Override
	public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
		_LOG.info("afterBulk " + response.getTook() + " " + (System.currentTimeMillis() - atomMap.get(executionId))
				+ "ms executionId "+executionId);
		
		
		
	/*	for (BulkItemResponse bulkItemResponse : response) {
			
			_LOG.info("response.getItems()----  "+bulkItemResponse.getId());
			
		}*/
		if(response.hasFailures()){
			_LOG.warn("buildFailureMessage "+response.buildFailureMessage());

		}
		
		atomMap.remove(executionId);
	}
	
	

}
