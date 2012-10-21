/**
 * 
 */
package com.browseengine.bobo.analysis.section;

import java.io.IOException;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.util.BytesRef;

/**
 * TokenStream for the section meta data. This returns a single token with a payload.
 */
public final class IntMetaDataTokenStream extends TokenStream
{
  private final String _tokenText;
  private final CharTermAttribute _termAttribute;
  private final OffsetAttribute _offsetAttribute;
  private final PayloadAttribute _payloadAtt;
  private BytesRef _payload;  
  private boolean _returnToken = false;

  public IntMetaDataTokenStream(String tokenText)
  {
    _tokenText = tokenText;
    _termAttribute = (CharTermAttribute)addAttribute(CharTermAttribute.class);
    _offsetAttribute = (OffsetAttribute)addAttribute(OffsetAttribute.class);
    _payloadAtt = (PayloadAttribute)addAttribute(PayloadAttribute.class);
  }

  /**
   * sets meta data
   * @param data array of integer metadata indexed by section id
   */
  public void setMetaData(int[] data)
  {
    byte[] buf = new byte[data.length * 4];
    int i = 0;
    
    for(int j = 0; j < data.length; j++)
    {
      int datum = data[j];
      buf[i++] = (byte)(datum);
      buf[i++] = (byte)(datum >>> 8);
      buf[i++] = (byte)(datum >>> 16);
      buf[i++] = (byte)(datum >>> 24);
    }
    
    _payload = new BytesRef(buf);
    _returnToken = true;
  }

  /**
   * Return the single token created.
   */
  public boolean incrementToken() throws IOException
  {
    if(_returnToken)
    {
      char[] chars = _tokenText.toCharArray();
      _termAttribute.copyBuffer(chars, 0, chars.length);
      _offsetAttribute.setOffset(0, 0);
      _payloadAtt.setPayload(_payload);
      _returnToken = false;
      return true;
    }
    return false;
  }
}
