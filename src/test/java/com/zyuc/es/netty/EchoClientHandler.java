/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.zyuc.es.netty;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import com.google.common.io.Files;

/**
 * Handler implementation for the echo client.  It initiates the ping-pong
 * traffic between the echo client and server by sending the first message to
 * the server.
 */
public class EchoClientHandler extends SimpleChannelUpstreamHandler {

    private static final Logger logger = Logger.getLogger(
            EchoClientHandler.class.getName());

    private final AtomicLong transferredBytes = new AtomicLong();

  //  static  String str="1370267492.934    182 121.14.234.147 TCP_MISS/000 - GET http://www.icbc.com.cn/ICBC/site/click/adverRedi.aspx?para=%2ficbc%2f%25e5%25b9%25bf%25e5%2591%258a%25e9%25a1%25b5%25e9%259d%25a2%2f%25e7%25bd%2591%25e7%25ab%2599%25e5%25ae%25a3%25e4%25bc%25a0%25e5%25b9%25bf%25e5%2591%258a%25e9%25a1%25b5%25e9%259d%25a2%2f%25e7%25bd%2591%25e7%25ab%2599%25e5%25b9%25bf%25e5%2591%258a%25e9%25a1%25b5%25e9%259d%25a2%2f2013%2f0502%25e8%25b4%25a6%25e6%2588%25b7%25e8%25b4%25b5%25e9%2587%2591%25e5%25b1%259e%25e6%259c%2589%25e5%25a5%2596%25e7%25ad%2594%25e9%25a2%2598%25e6%25b4%25bb%25e5%258a%25a8%2f0502%25e8%25b4%25a6%25e6%2588%25b7%25e8%25b4%25b5%25e9%2587%2591%25e5%25b1%259e%25e6%259c%2589%25e5%25a5%2596%25e7%25ad%2594%25e9%25a2%2598%25e6%25b4%25bb%25e5%258a%25a8.htm?1=1&Ad_Source=%e4%b8%bb%e5%9b%be_%e7%bb%bc%e5%90%88%e7%89%88%e9%a6%96%e9%a1%b5_450_280  - DIRECT/219.142.91.81 - \"-\" \"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)\" -";
  
    
    /**
     * Creates a client-side handler.
     */
    public EchoClientHandler(int firstMessageSize) {
        if (firstMessageSize <= 0) {
            throw new IllegalArgumentException(
                    "firstMessageSize: " + firstMessageSize);
        }
 
       
      
    }

    public long getTransferredBytes() {
        return transferredBytes.get();
    }

    @Override
    public void channelConnected(
            ChannelHandlerContext ctx, ChannelStateEvent e) {
    	sendMsg(e.getChannel());
    }
    
    public void sendMsg(Channel channel){
    	 String str;
  		
  			try {
  			   String path = EchoClientHandler.class.getResource("nettyTest").getPath(); 

  			   System.out.println("---"+path);
  			   System.out.println("------"+Thread.currentThread().getContextClassLoader().getResource(""));
  			   
				str = Files.toString(new File(path),Charset.forName("UTF-8"));
				ChannelBuffer buffer=	ChannelBuffers.dynamicBuffer();
	  			buffer.writeBytes(str.getBytes("UTF-8"));	  		
	  			channel.write(buffer).syncUninterruptibly();
			} catch (IOException e) {
				System.out.println("EchoClientHandler.sendMsg() "+e.getMessage());
			}
  			

    }

    @Override
    public void messageReceived(
            ChannelHandlerContext ctx, MessageEvent e) {
    
    
    	ChannelBuffer buffer=(ChannelBuffer) e.getMessage();
    	System.out.println("Receive:"+buffer.toString(Charset.forName("UTF-8")));
    	
    	//int l = buffer.readInt();
		//System.out.println("EchoServerHandler.messageReceived() l " + l);
		//int index = buffer.readerIndex();
		//int size=buffer.readBytes(l).readableBytes();

		//System.out.println("index " + index);
		//System.out.println(buffer.readableBytes());


	    	sendMsg(e.getChannel());

		

    }

    @Override
    public void exceptionCaught(
            ChannelHandlerContext ctx, ExceptionEvent e) {
   
    	StackTraceElement[] st=Thread.currentThread().getStackTrace();
    	
    /*	for (StackTraceElement stackTraceElement : st) {
			System.out.println(stackTraceElement);
		}*/
/*   System.out.println( ctx.getChannel().isConnected());
   System.out.println( ctx.getChannel().isOpen());
   System.out.println( ctx.getChannel().isWritable());
   System.out.println( ctx.getChannel().isWritable());
   
   System.out.println( ctx.getChannel());

   System.out.println(e.getChannel());

   
   System.out.println( ctx.getName()+"  "+ctx.getHandler());
    	System.out.println("EchoClientHandler.exceptionCaught() "+e.getCause().getMessage());*/
    	
    	
        e.getCause().printStackTrace();
        e.getChannel().close();
    }
}
