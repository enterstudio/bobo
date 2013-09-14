package com.browseengine.bobo.api.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene42.Lucene42Codec;

public class BoboCodec extends FilterCodec {

	private static final String CODEC_NAME = "BoboCodec";
	private static final Codec luceneCodec = new Lucene42Codec();
	
	private static final BoboFacetPostingsFormat postingsFormat = new BoboFacetPostingsFormat();
	public BoboCodec() {
		super(CODEC_NAME, luceneCodec);
	}

  @Override
  public DocValuesFormat docValuesFormat() {
    return new BoboDocValuesFormat(luceneCodec.docValuesFormat());
  }

  @Override
  public PostingsFormat postingsFormat() {
    return postingsFormat;
  }
	
  
}
