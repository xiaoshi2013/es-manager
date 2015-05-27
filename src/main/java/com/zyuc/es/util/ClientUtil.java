package com.zyuc.es.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.zyuc.es.input.client.MiniIndexService;

public class ClientUtil {

	private static final Logger _LOG = Logger.getLogger(ClientUtil.class);

	
	public final static AtomicReference<TransportClient> clientReference = new AtomicReference<>();

	private static volatile boolean b = true;
	private static volatile boolean started = false;
	
	public static Client getClient(){
		
		while (clientReference.get() == null || clientReference.get().connectedNodes().size() == 0) {
			_LOG.info("ClientUtil.getClient() "+clientReference.get());
			try {
				TimeUnit.SECONDS.sleep(3);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			
		}
		
		return clientReference.get();
	}
	
	

	public static synchronized void initClient(final String esname, final String eshostStr) {

		_LOG.debug("esname-------------- "+esname);
		_LOG.debug("eshostStr----------- "+eshostStr);
		
		if(started){
			_LOG.info("Client has started "+ClientUtil.getClient());
			return;
		}
		
		init(esname, eshostStr);

		Thread t = new Thread(new Runnable() {

			@Override
			public void run() {
				while (b) {
					if (clientReference.get() == null || clientReference.get().connectedNodes().size() == 0) {
						init(esname, eshostStr);
					}
					try {
						TimeUnit.SECONDS.sleep(3);
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}

				}
				_LOG.info("clientReference is null "+(clientReference.get()==null));

			}
		});

		t.start();

	}

	private static void init(String esname, String eshostStr) {
		try {
			Settings settings = ImmutableSettings.settingsBuilder().put("client.transport.ping_timeout", 30000)
					.put("client.transport.sniff", true).put("cluster.name", esname).build();

			TransportClient client = new TransportClient(settings);

			String[] eshost = eshostStr.split(",");

			for (int i = 0; i < eshost.length; i++) {
				client.addTransportAddress(new InetSocketTransportAddress(eshost[i], 9300));
			}

			ImmutableList<DiscoveryNode> list = client.connectedNodes();

			for (DiscoveryNode discoveryNode : list) {
				System.out.println(discoveryNode.getHostAddress() + " " + discoveryNode.getHostName() + " " + discoveryNode.getAddress());
			}

			System.out.println("Warning " + new DateTime().toLocalDateTime() + " ClientUtil.initClient() client.connectedNodes().size()==" + client.connectedNodes().size());
			clientReference.set(client);
			started=true;
		} catch (Throwable e) {
			System.err.println("ClientUtil.initClient eror ---" + e.getMessage());
		}
	}
	
	public static synchronized void close(){
		b=false;
		
		Client client=clientReference.get();
		if (client != null) {
			client.close();
			client=null;
			
		}
		else{
			_LOG.info("ClientUtil.close() Client is null ");

		}
		started=false;
		_LOG.info("ClientUtil.close() "+b);
	}

}
