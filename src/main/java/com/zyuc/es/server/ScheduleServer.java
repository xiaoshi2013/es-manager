package com.zyuc.es.server;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.channel.socket.oio.OioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;

import com.zyuc.es.util.ClientUtil;
import com.zyuc.es.util.Initialize;



/**
 * 查询和索引的服务端 
 * 如果嵌到soc里 可配在spring的xml里 同IndexNameManager 的配置
 * @author zhanglei
 *
 */
public class ScheduleServer {
	
	private static final Logger LOG = Logger.getLogger(ScheduleServer.class);
	
    int workerCount = Math.min(2, Runtime.getRuntime().availableProcessors());
     
	final ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1);
	final ScheduledExecutorService indexScheduled = Executors.newScheduledThreadPool(1);
	
	public ScheduleServer(){
		doStart();
	}
	public void doStart(){
		 ClientUtil.initClient(Initialize.esname, Initialize.eshost);

		Thread t = new Thread(new Runnable() {

			@Override
			public void run() {
				int port = Initialize.readserverport;
				UpdateIndexDate updateIndexDate = new UpdateIndexDate(scheduled);
				scheduled.schedule(updateIndexDate, 1, TimeUnit.SECONDS);
				ReadServerResponseHandler handler = new ReadServerResponseHandler(updateIndexDate);
				createServerBootstrap(port, handler);

			}
		},"readNameServer");

		Thread t1 = new Thread(new Runnable() {

			@Override
			public void run() {
				int port = Initialize.indexserverport;
				LatestIndicesServiceThread latestIndicesServiceThread = new LatestIndicesServiceThread(scheduled);
				indexScheduled.schedule(latestIndicesServiceThread, 1, TimeUnit.SECONDS);
				IndexServerResponseHandler handler = new IndexServerResponseHandler(latestIndicesServiceThread);
				createServerBootstrap(port, handler);

			}
		},"indexNameServer");
	
		t.start();
		t1.start();
		

		
	}
	
	private void createServerBootstrap(int port, final SimpleChannelUpstreamHandler handler){
		boolean blockingServer = false;
        
        ServerBootstrap serverBootstrap;
        if (blockingServer) {
            serverBootstrap = new ServerBootstrap(new OioServerSocketChannelFactory(
                    Executors.newCachedThreadPool(),
                    Executors.newCachedThreadPool()
            ));
        } else {
            serverBootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
                    Executors.newCachedThreadPool(),
                    Executors.newCachedThreadPool(),
                    workerCount));
        }
        serverBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() throws Exception {
            	   ChannelPipeline pipeline = Channels.pipeline();
            	   pipeline.addLast("lengthPrepender", new LengthFieldPrepender(4));
            	   pipeline.addLast("lengthDecoder",new LengthFieldBasedFrameDecoder(64*1024,0,4,0,4));
  	             pipeline.addLast("read", handler);  
  	             
                return pipeline;
            }
        });
        
        
    
        try {
            serverBootstrap.bind(new InetSocketAddress(port));      
            LOG.info("create client "+serverBootstrap+" ok");
		} catch (Exception e) {
			LOG.error("bind error",e);
		}
	}
	
	

    
  
    
    public static void main(String[] args) throws Exception {
    	ScheduleServer server=new ScheduleServer();
    	
    	
    	
    	
    	
    	
    	
    	
	}

  
}
