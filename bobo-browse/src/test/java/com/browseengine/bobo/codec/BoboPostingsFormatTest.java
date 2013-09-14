package com.browseengine.bobo.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.BasePostingsFormatTestCase;
import org.apache.lucene.util._TestUtil;

import com.browseengine.bobo.api.codec.BoboFacetPostingsFormat;

public class BoboPostingsFormatTest extends BasePostingsFormatTestCase{
  private final Codec codec = _TestUtil.alwaysPostingsFormat(new BoboFacetPostingsFormat());
  
  @Override
  protected Codec getCodec() {
    return codec;
  }
    
}
