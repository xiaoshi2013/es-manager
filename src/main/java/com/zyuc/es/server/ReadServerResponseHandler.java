package com.zyuc.es.server;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.common.joda.time.DateTime;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.buffer.DynamicChannelBuffer;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.google.common.collect.Sets;
import com.zyuc.es.util.Initialize;
import com.zyuc.es.util.Utils;

public class ReadServerResponseHandler extends SimpleChannelUpstreamHandler {

	private static final Logger LOG = Logger.getLogger(ReadServerResponseHandler.class);

	private UpdateIndexDate updateIndexDate;

	public ReadServerResponseHandler(UpdateIndexDate updateIndexDate) {
		this.updateIndexDate = updateIndexDate;
	}

	@Override
	public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		LOG.debug("ResponseHandler.channelDisconnected()");
		super.channelDisconnected(ctx, e);
	}

	@Override
	public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		LOG.debug("ResponseHandler.channelClosed()");
		super.channelClosed(ctx, e);
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {

		ChannelBuffer buffer = (ChannelBuffer) e.getMessage();
		// System.out.println( buffer.readableBytes());
		String msg = buffer.toString(Charset.forName("UTF-8"));

		Map map = JSON.parseObject(msg, Map.class);

		Object o = map.get(Initialize.SERVICE);

		String sid = map.get(Initialize.SESSIONID).toString();

		String error = "";
		Map res = new HashMap();
		if (o == null) {
			error = "session " + sid + " service is null";

			res.put("error", error);
			res.put(Initialize.SESSIONID, sid);

		} else {
			while (updateIndexDate.getInvocations().get()==0 
					|| updateIndexDate.getService_indices() == null 
					|| updateIndexDate.getServices() == null ) {
				LOG.info("wait...");
				try {
					// LOG.debug("updateIndexDate.getService_indices() "+updateIndexDate.getService_indices());
					TimeUnit.SECONDS.sleep(100);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
			String service = o.toString();
			service = StringUtils.removeEnd(service, "-");

			String[] ss = null;
			Set<String> set = updateIndexDate.getService_indices().get(service);
			if ((Boolean) map.get("all") == true) {
				if (set.size() > 0) {
					ss = set.toArray(new String[set.size()]);
				}
			} else {
				Set<String> indexName = Sets.newLinkedHashSet();

				long from = Long.parseLong(map.get("from").toString());
				long to = Long.parseLong(map.get("to").toString());

				DateTime dtfrom = new DateTime(from);

				from = dtfrom.toLocalDate().toDateTimeAtStartOfDay().getMillis();
				if (to <= 0 || to >= DateTime.now().getMillis()) {
					to = DateTime.now().getMillis();
				}

				DateTime dtto = new DateTime(to);
				to = dtto.toLocalDate().toDateTimeAtStartOfDay().getMillis();

				// LOG.debug(dtfrom+" "+dtto);

				if (updateIndexDate.getIndex_date() != null) {
					for (String index : set) {
						Collection<Long> coll = updateIndexDate.getIndex_date().get(index);
						if (coll == null || coll.size() == 0) {
							continue;
						}
						Long[] times = coll.toArray(new Long[coll.size()]);

						Arrays.sort(times);
						for (Long long1 : times) {
							// LOG.debug(new DateTime(long1)+"   "+index);

							if (long1 >= from && long1 <= to) {
								indexName.add(index);
							}
						}
					}

					if (indexName.size() > 0) {
						ss = indexName.toArray(new String[indexName.size()]);
					}
				} else {
					//ss = getIndexNamesFromBin(set, from, to);
					if ((ss == null || ss.length == 0) && set.size() > 0) {
						ss = set.toArray(new String[set.size()]);
						LOG.debug("get the All IndexNames " + Arrays.toString(ss));
					}

				}

			}

			if(ss==null){
				ss=new String[1];
			}
			res.put(Initialize.SESSIONID, sid);
			res.put(Initialize.INDICES, ss);

		}

		String resstr = JSON.toJSONString(res, SerializerFeature.WriteMapNullValue);

		// LOG.warn("read ----- "+resstr);
		try {
			ChannelBuffer message = new DynamicChannelBuffer(1024);

			message.writeBytes(ChannelBuffers.wrappedBuffer(resstr.getBytes("UTF-8")));
			ChannelFuture future = e.getChannel().write(message);

			future.syncUninterruptibly();

		} catch (UnsupportedEncodingException e1) {
			e1.printStackTrace();
		}
	}

	/*public String[] getIndexNamesFromBin(Collection<String> indices, long from, long to) {
		Set<String> indexName = Sets.newLinkedHashSet();

		Map<String, Collection<Long>> index_date_ = Utils.readServerDeserialization();

		if (index_date_ == null) {
			return null;
		}
		for (String index : indices) {
			Collection<Long> coll = index_date_.get(index);
			if (coll == null || coll.size() == 0) {
				continue;
			}
			Long[] times = coll.toArray(new Long[coll.size()]);
			Arrays.sort(times);
			for (Long long1 : times) {
				if (long1 >= from && long1 <= to) {
					indexName.add(index);
				}
			}
		}

		return indexName.toArray(new String[indexName.size()]);

	}*/

	

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
		// Close the connection when an exception is raised.
		e.getCause().printStackTrace();
		e.getChannel().close();
	}

}
