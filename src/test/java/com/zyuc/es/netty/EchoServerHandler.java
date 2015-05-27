
package com.zyuc.es.netty;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

/**
 * Handler implementation for the echo server.
 */
public class EchoServerHandler extends SimpleChannelUpstreamHandler {

    private static final Logger logger = Logger.getLogger(
            EchoServerHandler.class.getName());

    private final AtomicLong transferredBytes = new AtomicLong();

    public long getTransferredBytes() {
        return transferredBytes.get();
    }

    @Override
    public void messageReceived(
            ChannelHandlerContext ctx, MessageEvent e) {
        // Send back the received message to the remote peer.
    
    /*	System.out.println("EchoServerHandler.messageReceived()");
    	ChannelBuffer buffer=(ChannelBuffer) e.getMessage();
    
    	
    	int l = buffer.readInt();

    	System.out.println("EchoServerHandler.messageReceived() l "+l);
		int size=buffer.readBytes(l).readableBytes();
		System.out.println("size "+size);
		System.out.println("buffer.readerIndex() "+buffer.readerIndex());

		try {
			
			String str=new String(buffer.readBytes(l).array(), "UTF-8");
			
			sendMsg(str, e.getChannel());
		} catch (UnsupportedEncodingException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
*/
    	
    	ChannelBuffer buffer = (ChannelBuffer)e.getMessage();
    	System.out.println(	buffer.readableBytes());
    	//System.out.println("Receive:"+buffer.toString(Charset.forName("UTF-8")));
    	String msg = buffer.toString(Charset.forName("UTF-8")) + "has been processed!";
    	ChannelBuffer buffer2 = ChannelBuffers.buffer(msg.length());
    	buffer2.writeBytes(msg.getBytes());
    	e.getChannel().write(buffer2);
    	
    }
    
    
    public void sendMsg(String str,Channel channel){
 		
 			try {
			
 				StringBuilder sb=new StringBuilder();
 				for (int i = 0; i < 20; i++) {
 					sb.append(str);
				}
				ChannelBuffer buffer=ChannelBuffers.dynamicBuffer();
	  			buffer.writeBytes(sb.toString().getBytes("UTF-8"));
	  			channel.write(buffer);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
 			

   }

    @Override
    public void exceptionCaught(
            ChannelHandlerContext ctx, ExceptionEvent e) {
        // Close the connection when an exception is raised.
        logger.log(
                Level.WARNING,
                "Unexpected exception from downstream.",
                e.getCause());
        e.getChannel().close();
    }
    
    public void channelDisconnected(
            ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    	System.out.println("EchoServerHandler.channelDisconnected() "+transferredBytes.get());
        ctx.sendUpstream(e);
    }
}
