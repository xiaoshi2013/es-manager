package com.zyuc.es.analyzer;

import java.io.Reader;

import org.apache.lucene.analysis.util.CharTokenizer;
import org.apache.lucene.util.Version;

public class MyWhitespaceTokenizer extends CharTokenizer  {

	 
	  private static final String PUNCTUATION = " !\"',;?)([]<>*#\n\t\r=";

	  
	  public MyWhitespaceTokenizer(Version matchVersion, Reader in) {
	    super(matchVersion, in);
	  }

	
	  public MyWhitespaceTokenizer(Version matchVersion, AttributeFactory factory, Reader in) {
	    super(matchVersion, factory, in);
	  }
	  

	  @Override
	  protected boolean isTokenChar(int c) {
	   return  PUNCTUATION.indexOf(c)==-1;
		  //return !Character.isWhitespace(c);
		 
	  }
}
