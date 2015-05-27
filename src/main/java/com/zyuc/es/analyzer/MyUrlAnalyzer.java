package com.zyuc.es.analyzer;

import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.miscellaneous.WordDelimiterFilter;
import org.apache.lucene.analysis.standard.ClassicFilter;
import org.apache.lucene.analysis.standard.ClassicTokenizer;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.StopwordAnalyzerBase;
import org.apache.lucene.util.Version;

public class MyUrlAnalyzer extends StopwordAnalyzerBase {
	  /** Default maximum allowed token length */
	  public static final int DEFAULT_MAX_TOKEN_LENGTH = 255;

	  private int maxTokenLength = DEFAULT_MAX_TOKEN_LENGTH;

	  /** An unmodifiable set containing some common English words that are usually not
	  useful for searching. */
	  public static final CharArraySet STOP_WORDS_SET = StopAnalyzer.ENGLISH_STOP_WORDS_SET; 

	  /** Builds an analyzer with the given stop words.
	   * @param matchVersion Lucene version to match See {@link
	   * <a href="#version">above</a>}
	   * @param stopWords stop words */
	  public MyUrlAnalyzer(Version matchVersion, CharArraySet stopWords) {
	    super(matchVersion, stopWords);
	  }

	  /** Builds an analyzer with the default stop words ({@link
	   * #STOP_WORDS_SET}).
	   * @param matchVersion Lucene version to match See {@link
	   * <a href="#version">above</a>}
	   */
	  public MyUrlAnalyzer(Version matchVersion) {
	    this(matchVersion, STOP_WORDS_SET);
	  }

	  /** Builds an analyzer with the stop words from the given reader.
	   * @see WordlistLoader#getWordSet(Reader, Version)
	   * @param matchVersion Lucene version to match See {@link
	   * <a href="#version">above</a>}
	   * @param stopwords Reader to read stop words from */
	  public MyUrlAnalyzer(Version matchVersion, Reader stopwords) throws IOException {
	    this(matchVersion, loadStopwordSet(stopwords, matchVersion));
	  }

	 
	  public void setMaxTokenLength(int length) {
	    maxTokenLength = length;
	  }
	    
	 
	  public int getMaxTokenLength() {
	    return maxTokenLength;
	  }

	  @Override
	  protected TokenStreamComponents createComponents(final String fieldName, final Reader reader) {
	    final MyWhitespaceTokenizer src = new MyWhitespaceTokenizer(matchVersion, reader);
	    
	    //final ClassicTokenizer src = new ClassicTokenizer(matchVersion, reader);
	    
	  //  src.setMaxTokenLength(maxTokenLength);
	    
		    
		   
	    TokenStream tok = new StandardFilter(matchVersion, src);
	    tok = new MyTrimFilter( tok);
	    tok = new LowerCaseFilter(matchVersion, tok);

	   tok = new StopFilter(matchVersion, tok, stopwords);

	    
	    return new TokenStreamComponents(src,tok) {
	      @Override
	      protected void setReader(final Reader reader) throws IOException {
	       // src.setMaxTokenLength(MyUrlAnalyzer.this.maxTokenLength);
	        super.setReader(reader);
	      }
	    };
	  }
	

}
