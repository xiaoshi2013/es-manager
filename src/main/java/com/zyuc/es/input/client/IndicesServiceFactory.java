package com.zyuc.es.input.client;

import org.apache.log4j.Logger;
import org.elasticsearch.client.Client;

import com.zyuc.es.util.ClientUtil;
import com.zyuc.es.util.Initialize;

/**
 * Simple static factories create singleton service object
 * @author zhanglei
 *
 */
public class IndicesServiceFactory {

	private static Logger _LOG = Logger.getLogger(IndicesServiceFactory.class);

	private static IndicesService indicesService;

	private static final Object mutex = new Object();

	public static IndicesService getIndicesService() {
		return indicesService;
	}

	/**
	 * Returns a singleton IndicesService automatically checks whether a single case
	 * @param useZK The default is false
	 * @return
	 * @throws Exception
	 */
	public static IndicesService createIndicesService() throws Exception {

		synchronized (mutex) {
			if (indicesService != null) {
				_LOG.error("IndicesService " + indicesService + " is already exists  exits...");
				return indicesService;
			}

		
		
				return createMiniIndexService();
			

		}

	}

	private static IndicesService createMiniIndexService() throws Exception {
		indicesService = MiniIndexService.getInstance();
		((MiniIndexService) indicesService).doStart();
		return indicesService;
	}



}
