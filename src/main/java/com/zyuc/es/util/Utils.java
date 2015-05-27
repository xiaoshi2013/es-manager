package com.zyuc.es.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.LocalDateTime;
import org.elasticsearch.common.joda.time.format.DateTimeFormat;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.HashMultimap;

public class Utils {
	
	private static Logger LOG = Logger.getLogger(Utils.class);

	
	public static final String zknamespace="esmanage";
	public static final String zkpath="/indices";
	public static final String zklatestpath="/latest";
	public static final String zkdatepath="/index_date";

	public static  String TIMESTAMP="@timestamp";

	
	//public static   String zkConn;

	/*static{
		 Properties prop = new Properties();
		   String path = Thread.currentThread().getContextClassLoader().getResource("").getPath(); 
		   path=path+"client-init.properties";
			_LOG.info("path "+path);
		   
			
			try {
				prop.load(new FileInputStream(path));
			} catch ( IOException e) {
				
				throw new RuntimeException(e);
			}
			
		zkConn=prop.getProperty("zkConn");
	}*/

/*	*//**
	 * 有可能和索引名一样
	 * 没有时间后缀的返回索引名
	 * 不论是下划线还是横杠都去掉 返回索引名
	 * @param indexName
	 * @param dt
	 * @return service name
	 * @throws Exception
	 *//*
	public static String parsing(String indexName){
	
		DateTime dt=DateTime.now();
		int year= dt.getYear()+1;
		int n=-1;
		for (int i = 0; i < 20; i++) {
			n=indexName.indexOf(year+"");
		
			if(n<0){
				year=year-1;
				
				n=indexName.indexOf(year+"");
			}
			else{
				break;
			}
		}
		
		if(n<0){
			// 没有时间后缀
			//System.out.println("Index name No template: "+indexName);
			
			//noSuffix_indices.add(indexName);
			// 没有时间后缀的直接返回索引名
			
		String service=StringUtils.substringBeforeLast(indexName, "_");
			String no=StringUtils.substringAfterLast(indexName, "_");
			if(no.length()<=2 && no.length()>=1){
				int num=Integer.parseInt(no);
				
				return service;
			}
			
			return indexName;
			
			
			
		}
		
		//有时间后缀
		String suffix=indexName.substring(indexName.indexOf(year+""));
		
		String service= indexName.substring(0,indexName.indexOf(suffix));
		 if(service.endsWith("-") ||service.endsWith("_") ){
			 service= service.substring(0, service.length()-1);
		 }
		 
		 return service;
	}*/
	
/*	public static String matchPattern(String dateStr){
		
		String str=	StringUtils.remove(dateStr, ".");
		str=StringUtils.remove(str, "-");
		
		//System.out.println("matchPattern "+dateStr+"  "+str);
		
		if(!StringUtils.isNumeric(str)){
			return StringUtils.EMPTY;
		}
		
		
		if(StringUtils.countMatches(dateStr, ".")==2){
			return "yyyy.MM.dd";
		}
		
		else if(StringUtils.countMatches(dateStr, "-")==2){
			return "yyyy-MM-dd";
		}
		
		else if(StringUtils.countMatches(dateStr, ".")==1){
			return "yyyy.MM";
		}
		
		else if(StringUtils.countMatches(dateStr, "-")==1){
			return "yyyy-MM";
		}
		
		if(dateStr.length()==8 && dateStr.indexOf("-")==-1 &&  dateStr.indexOf(".")==-1){
			return "yyyyMMdd";
		}
		
		else if(dateStr.length()==4){
			return "yyyy";
		}
		else if(dateStr.length()==6){
			return "yyyyMM";

		}
		
		
		return StringUtils.EMPTY;
	}*/
	
	/**
	 * 生成指定天数之前到当前日期的起止日期的long值
	 * @param period
	 * @return
	 */
	public static org.elasticsearch.common.collect.Tuple<Long,Long> generateFromAndToDate(int period){
		long from =new LocalDateTime().minusDays(period).toLocalDate().toDate().getTime();
		long to =new DateTime().plusDays(1).toLocalDate().toDateTimeAtStartOfDay().getMillis()-1;
		return org.elasticsearch.common.collect.Tuple.tuple(from, to);
		
	}
	

	 	/** *//**文件重命名 
	    * @param path 文件目录 
	    * @param oldname  原来的文件名 
	    * @param newname 新文件名 
	    */ 
	    public static void renameFile(String oldname,String newname) throws Exception{
	        if(!oldname.equals(newname)){
	            File oldfile=new File(oldname); 
	            File newfile=new File(newname); 
	            if(newfile.exists()){
	            	newfile.delete();
	            }
	            oldfile.renameTo(newfile); 
	        }        
	    }
	    
	    
	    public static String noSuffix(String indexName){ 
			// ace_1 ace
			String name="";
			String no="";
			String service="";
			 no=StringUtils.substringAfterLast(indexName, "_");
			 if(no.length()>0 && no.length()<=2 && StringUtils.isNumeric(no)){
				 
				 int n=Integer.parseInt(no);
				 n++;
				 no=n+"";
				 service= StringUtils.substringBeforeLast(indexName, "_");
			 }
			 else{
				 // 没后缀
				 no="1";
				service=indexName;
			 }
			 
			 
		
			 name=service+"_"+no;
			
			 
			
			
			return name;
		}
	    
		/**
		 * 区别 service 带下划线的情况 同一日期 加序号 不同日期直接返回
		 * @param service
		 * @param indexName
		 * @return
		 * @throws Exception 
		 */
		public static String dealUnderline(String service,String indexName) throws Exception{
			String name="";
			String no="";
			String suffix="";
			
			service=StringUtils.removeEnd(service, "_");
			service=StringUtils.removeEnd(service, "-");
			
			String odate = StringUtils.remove(indexName, service);
			suffix=StringUtils.removeStart( odate,"-"); 
			suffix=StringUtils.removeStart( odate,"_"); 
			
			char delimiter=odate.charAt(0);			
			odate = normalization(odate);
			System.out.println("Utils.dealUnderline() odate "+odate);

			
			
		
			String str=Pattern.matchPattern(odate);
			if(str.equals("")){
				throw new Exception(indexName +" suffix Illegal :"+odate);
			}
			Format format=Format.formatMap.get(str);
			String dateStr=DateTime.now().toString(DateTimeFormat.forPattern(format.getFormatter()));
			
			// 时间段相同 加序号
			if(dateStr.equals(odate)){
				no=StringUtils.substringAfterLast(suffix, "_");			 
				if(no.length()<=2 && no.length()>0 && StringUtils.isNumeric(no)){
					int n=Integer.parseInt(no);
					n++;
					no=n+"";
				}
				else{
					no="1";
				}
				name=service+delimiter+dateStr+"_"+no;
				
			}
			else{
				name=service+delimiter+dateStr;
			}
			
			return name;
			
			
		}
		
		static String normalization(String dateStr) {

			if (dateStr.startsWith("-") || dateStr.startsWith("_")) {
				dateStr = dateStr.substring(1);

			}

			// 判断序号 和索引名本身就带下划线的
			if (dateStr.lastIndexOf("_") > 0) {

				if (StringUtils.substringAfterLast(dateStr, "_").length() <= 2) {
					dateStr = StringUtils.substringBeforeLast(dateStr, "_");

				}

			}

			return dateStr;

		}


		/**
		 * 缓存序列化到文件
		 * @param index_date_
		 * @param index_docs
		 * @throws FileNotFoundException
		 */
		public static synchronized void readServerSerialization( HashMultimap<String, Long> index_date_ ,Map<String,Long> index_docs)
				throws FileNotFoundException{
			DateTime dt=new DateTime();
			String newname = "index_date.bin";
			File f = new File("index_date.bin.tmp");
			f.delete();
			Kryo kryo = new Kryo();
			kryo.setReferences(false);
			kryo.register(com.zyuc.es.util.Tuple.class);

			Output output;
			try {
				output = new Output(new FileOutputStream(f));

				Map<String, Collection<Long>> map_ = new HashMap();

				Set<String> keys = index_date_.keySet();
				for (String key : keys) {
					Collection<Long> values = Lists.newArrayList();
					values.addAll(index_date_.get(key));
					map_.put(key, values);
				}

				Tuple tuple = new Tuple(index_docs, map_);
				kryo.writeObject(output, tuple);
				output.flush();
				output.close();
				Utils.renameFile(f.getName(), newname);
			} catch (Exception e) {
				f.delete();
				new File(newname).delete();
				LOG.error("error",e);

			}
		

		}
		
		/**
		 * 读取缓存的索引和日期的映射文件 
		 * 如果没有返回null
		 * @return
		 */
		public static  Tuple readServerDeserialization() {
			long start = System.currentTimeMillis();
			Kryo kryo = new Kryo();
			kryo.setReferences(false);
			kryo.register(com.zyuc.es.util.Tuple.class);
			File f = new File("index_date.bin");
			if (f.exists()) {
				try {
					Input input = new Input(new FileInputStream(f));
					Tuple tuple=null;
					while (!input.eof()) {
						tuple = kryo.readObject(input, Tuple.class);
					}
					input.close();

					LOG.info("Deserialization index_date.bin " + (System.currentTimeMillis() - start) + " ms");
					//LOG.info("index_date.bin  " + index_date_.keySet());
					return tuple;
				} catch (FileNotFoundException e) {

					LOG.error("error",e);
				}

			}
			return null;
		}
		
		
		public static synchronized void indexServerSerialization( Map<String, Long> indexLasttime,Map<String,Long> index_docs)
				throws FileNotFoundException{
			long start = System.currentTimeMillis();
			DateTime dt=new DateTime();
			String newname = "index_lasttime.bin";
			File f = new File("index_lasttime.bin.tmp");
			f.delete();
			Kryo kryo = new Kryo();
			kryo.setReferences(false);
			kryo.register(com.zyuc.es.util.Tuple.class);

			Output output;
			try {
				output = new Output(new FileOutputStream(f));
				Tuple tuple = new Tuple(index_docs, indexLasttime);
				kryo.writeObject(output, tuple);
				output.flush();
				output.close();
				Utils.renameFile(f.getName(), newname);
				//LOG.info("indexServerSerialization index_lasttime.bin " + (System.currentTimeMillis() - start) + " ms");

			} catch ( Exception e) {
				f.delete();
				new File(newname).delete();
				LOG.error("error",e);
			}
		

		}
		
		public static  Tuple indexServerDeserialization() {
			long start = System.currentTimeMillis();
			Kryo kryo = new Kryo();
			kryo.setReferences(false);
			kryo.register(com.zyuc.es.util.Tuple.class);
			File f = new File("index_lasttime.bin");
			if (f.exists()) {
				try {
					Input input = new Input(new FileInputStream(f));
					
					Tuple tuple =null;
					while (!input.eof()) {
						tuple = kryo.readObject(input, Tuple.class);
						
					}
					input.close();

					LOG.info("indexServerDeserialization index_lasttime.bin " + (System.currentTimeMillis() - start) + " ms");
					LOG.info(tuple.v1());
					LOG.info(tuple.v2());
					return tuple;
				} catch (FileNotFoundException e) {
					LOG.error("error",e);
				}

			}
			return null;
		}
		
}
