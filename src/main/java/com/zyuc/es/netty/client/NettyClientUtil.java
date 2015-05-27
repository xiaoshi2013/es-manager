package com.zyuc.es.netty.client;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.unit.TimeValue;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;

import com.zyuc.es.input.client.IndexClientHandler;
import com.zyuc.es.netty.client.NettyIndexService.Flag;
import com.zyuc.es.query.client.ReadClientHandler;
import com.zyuc.es.util.Initialize;

public class NettyClientUtil {
	
	

	private static final Logger LOG = Logger.getLogger(NettyClientUtil.class);

	TimeValue connectTimeout=new TimeValue(30, TimeUnit.SECONDS);

	Channel channel;

	ChannelFactory factory;
	
	private volatile boolean check=true;
	
	public volatile boolean error=false;
	
	
	ChannelGroup allChannels = new DefaultChannelGroup();

	private final AtomicLong requestCounter = new AtomicLong();


	private final Map<String,String[]> indexMap=Maps.newConcurrentMap();
	
	private NettyIndexService service;
	
	private int serverPort;
	
	private Flag flag;

	
	public NettyClientUtil(NettyIndexService service,Flag flag){
		this.service=service;
		this.flag=flag;
		if(flag==Flag.READ){
			this.serverPort=Initialize.readserverport;
		}
		else if(flag==Flag.INDEX){
			this.serverPort=Initialize.indexserverport;

		}
		checkConnections();

	}
	
	
	


	public void doStart(){
		
		Channel chan = connection();

		while (chan == null) {
			try {
				doStop();
				TimeUnit.SECONDS.sleep(5);
				chan = connection();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		channel = chan;
		allChannels.add(channel);
		LOG.debug(channel);
		error = false;
		

			
	}
	private Channel connection(){
		ClientBootstrap clientBootstrap= createClientBootstrap();

		ChannelFuture connect = clientBootstrap.connect(new InetSocketAddress(Initialize.serverhost, serverPort));
        connect.awaitUninterruptibly((long) (connectTimeout.millis() * 1.5));
        if(connect.isSuccess()){
        	return connect.getChannel();
        }
        return null;
	}

    private ClientBootstrap createClientBootstrap() {
    	 

    	 this.factory=new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), 
				Executors.newCachedThreadPool(), 2);
    	
		ClientBootstrap clientBootstrap = new ClientBootstrap(factory);

		clientBootstrap.setPipelineFactory(new ChannelPipelineFactory() {

			@Override
			public ChannelPipeline getPipeline() throws Exception {
				ChannelPipeline pipeline = Channels.pipeline();

				SimpleChannelUpstreamHandler handler=null;
		    	if(flag==Flag.READ){
		    		handler=new ReadClientHandler(service);
		    	}
		    	else if(flag==Flag.INDEX){
		    		handler=new IndexClientHandler(service);
		    	}
		    	
				pipeline.addLast("lengthPrepender", new LengthFieldPrepender(4));
				pipeline.addLast("lengthDecoder", new LengthFieldBasedFrameDecoder(64 * 1024, 0, 4, 0, 4));
				pipeline.addLast("index",handler);
				return pipeline;
			}
		});

		clientBootstrap.setOption("connectTimeoutMillis", connectTimeout.millis());

		clientBootstrap.setOption("tcpNoDelay", true);

		clientBootstrap.setOption("keepAlive", true);

		clientBootstrap.setOption("receiveBufferSize", 1048576);
		clientBootstrap.setOption("sendBufferSize", 1048576);

		return clientBootstrap;
    }

    public void doStop(){
		try {
			   ChannelGroupFuture future = allChannels.close();  
		        future.awaitUninterruptibly();  
		        factory.releaseExternalResources();
		        this.channel=null;
		        this.allChannels.clear();
				LOG.debug("channel "+channel+" close" +"  "+ future.isCompleteSuccess());

	        
		} catch (Exception e) {
			LOG.error("close channle error",e);
		}
		
	}
    
    public void doClose(){
    	  check=false;
    }
	
	public void checkConnections(){
		Thread t=new Thread(new Runnable() {
			
			@Override
			public void run() {
				while (check) {
					LOG.trace(Thread.currentThread().getName()+ " ------------error is : "+error);
					if(error){
						doStop();
						doStart();
					}
					try {
						TimeUnit.SECONDS.sleep(5);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				}
				
			}
		});
		
		t.start();
	}

	public long increment(){
		return requestCounter.incrementAndGet();
	}


	public Map<String, String[]> getIndexMap() {
		return indexMap;
	}

	public Channel getChannel() {
		return channel;
	}
}
