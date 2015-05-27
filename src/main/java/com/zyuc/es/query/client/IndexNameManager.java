package com.zyuc.es.query.client;

import com.zyuc.es.input.client.IndicesService;
import com.zyuc.es.input.client.MiniIndexService;
import com.zyuc.es.util.Format;


public class IndexNameManager implements IndexReadService,IndicesService {

	private  IndexReadService inexIndexReadService;
	private  IndicesService indicesService;
	



	private static final Object mutex = new Object();

	public IndexReadService getInexIndexReadService() {
		return inexIndexReadService;
	}
	
	public IndicesService getIndicesService() {
		return indicesService;
	}

	public IndexNameManager() {

		synchronized (mutex) {
			if (inexIndexReadService == null) {
				MiniIndexReadService inexIndexReadService_ = MiniIndexReadService.getInstance();
				inexIndexReadService_.doStart();
				inexIndexReadService = inexIndexReadService_;
			}

			
			if(indicesService==null){
				MiniIndexService service=MiniIndexService.getInstance();
				service.doStart();
				indicesService=service;
			}
			
			
			

		}

	}

	@Override
	public String[] getIndexName(String service, long from, long to) {
		return inexIndexReadService.getIndexName(service, from, to);

	}

	@Override
	public String[] getIndexName(String service, String date) {
		return inexIndexReadService.getIndexName(service, date);
	}

	@Override
	public String[] getIndexName(String service, int period) {
		return inexIndexReadService.getIndexName(service, period);
	}

	@Override
	public String[] getIndexNameAll(String service) {

		String[] arr = inexIndexReadService.getIndexNameAll(service);

		if (arr == null || arr.length == 0) {
			return new String[] { service + "*" };
		}
		return arr;
	}

	@Override
	public String obtainLatestIndexName(String service, boolean isSingle) {
		return indicesService.obtainLatestIndexName(service, isSingle);
		
	}

	@Override
	public String obtainLatestIndexName(String service, boolean isSingle, Format format) {
		return indicesService.obtainLatestIndexName(service, isSingle, format);
		
	}

}
