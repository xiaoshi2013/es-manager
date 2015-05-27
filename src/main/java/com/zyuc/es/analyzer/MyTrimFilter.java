package com.zyuc.es.analyzer;

import java.io.IOException;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.ClassicTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.Version;

/**
 * Trims leading and trailing whitespace from Tokens in the stream.
 * <p>As of Lucene 4.4, this filter does not support updateOffsets=true anymore
 * as it can lead to broken token streams.
 */
public final class MyTrimFilter extends TokenFilter {

  final boolean updateOffsets;
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

  /**
   * Create a new {@link TrimFilter}.
   * @param version       the Lucene match version
   * @param in            the stream to consume
   * @param updateOffsets whether to update offsets
   * @deprecated Offset updates are not supported anymore as of Lucene 4.4.
   */
  @Deprecated
  public MyTrimFilter(Version version, TokenStream in, boolean updateOffsets) {
    super(in);
    if (updateOffsets && version.onOrAfter(Version.LUCENE_44)) {
      throw new IllegalArgumentException("updateOffsets=true is not supported anymore as of Lucene 4.4");
    }
    this.updateOffsets = updateOffsets;
  }

  /** Create a new {@link TrimFilter} on top of <code>in</code>. */
  public MyTrimFilter(TokenStream in) {
    super(in);
    this.updateOffsets = false;
  }

  /**
   * @deprecated Use {@link #TrimFilter(TokenStream)}
   */
  @Deprecated
  public MyTrimFilter(Version version, TokenStream in) {
    this(version, in, false);
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (!input.incrementToken()) return false;

    char[] termBuffer = termAtt.buffer();
    int len = termAtt.length();
    //TODO: Is this the right behavior or should we return false?  Currently, "  ", returns true, so I think this should
    //also return true
    if (len == 0){
      return true;
    }
    int start = 0;
    int end = 0;
    int endOff = 0;

    // eat the first characters
   // System.out.println("----- "+termBuffer[start]);
    for (start = 0; start < len && (Character.isWhitespace(termBuffer[start]) 
    		//|| (int)termBuffer[start]==45
    		); start++) {
    }
    // eat the end characters
    for (end = len; end >= start && (Character.isWhitespace(termBuffer[end - 1]) 
    		//|| (int)termBuffer[end-1]==45
    		); end--) {
      endOff++;
    }
   // System.out.println("-- "+start+" "+end);
    if (start > 0 || end < len) {
      if (start < end) {
        termAtt.copyBuffer(termBuffer, start, (end - start));
      } else {
        termAtt.setEmpty();
      }
      if (updateOffsets && len == offsetAtt.endOffset() - offsetAtt.startOffset()) {
        int newStart = offsetAtt.startOffset()+start;
        int newEnd = offsetAtt.endOffset() - (start<end ? endOff:0);
        offsetAtt.setOffset(newStart, newEnd);
      }
    }
    return true;
  }
}