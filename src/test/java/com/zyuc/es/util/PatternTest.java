package com.zyuc.es.util;

import org.apache.commons.lang.StringUtils;

public class PatternTest {
	
	
	
	public  static void testdetermineIncremental() throws Exception{
		
		String[] indices={"msg-2015-01-05_20","msg-2015-01-05_21","msg-2015-01-05_22","msg-2015-01-05"};
		String[] indices1={"ddos-20150104","ddos-20141223","ddos-20141223_1","ddos-20141223_2",
				"ddos-20140825","ddos-20140821","ddos-201408","ddos-2014-08-21","ddos-2014.12"};
		String[] indices2={"alarmstorm","alarmstorm-2014.09","alarmstorm-2014.10"};
		
		String[] indices3={"msg-2015-01-05"};
		String[] indices4={"msg","msg_1","msg_2"};
		
		String[] indices5={"ddos_src_analysis_2015.01","ddos_src_analysis_2014.12"};

		
		//String[] indices={"atstorm-2014.08.27","atstorm-2014.08.28","atstorm-2014.08.29","atstorm-2014.09.30","atstorm-2014.10.23"};

		
		//String[] indices={"ddos_src_analysis_2014.12"};
			
    	System.out.println(Pattern.determineIncremental(indices, "msg"));
    	System.out.println(Pattern.determineIncremental(indices1, "ddos"));
    	System.out.println(Pattern.determineIncremental(indices2, "alarmstorm"));
    	System.out.println(Pattern.determineIncremental(indices3, "msg"));
    	System.out.println(Pattern.determineIncremental(indices4, "msg"));
    	
    	System.out.println(Pattern.determineIncremental(indices5, "ddos_src_analysis"));

    	System.out.println("--------- "+Pattern.creatIncrementalIndex("ddos_src_analysis",Pattern.
    			determineIncremental(indices5, "ddos_src_analysis")));
    	
    	
	}

	public static void main(String[] args) throws Exception {
		testdetermineIncremental();
		
		
		System.out.println( Pattern.toFormat("msg-2015-02-357","msg"));
		
		StringUtils.split("");

	}
}
