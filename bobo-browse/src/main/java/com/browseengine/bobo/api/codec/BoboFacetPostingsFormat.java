package com.browseengine.bobo.api.codec;

import java.io.IOException;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene41.Lucene41PostingsReader;
import org.apache.lucene.codecs.lucene41.Lucene41PostingsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

public class BoboFacetPostingsFormat extends PostingsFormat {

  private static final String NAME = "BoboPostingsFormat";
  
  public BoboFacetPostingsFormat() {
    super(NAME);
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state)
      throws IOException {
    Lucene41PostingsWriter postingsWriter = new Lucene41PostingsWriter(state);
    return new BoboFacetFieldsConsumer(state, postingsWriter);
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state)
      throws IOException {
    Lucene41PostingsReader postingsReader = new Lucene41PostingsReader(state.directory, 
        state.fieldInfos, state.segmentInfo, state.context, state.segmentSuffix);
    
    return new BoboFacetFieldsProducer(state, postingsReader);
  }

}
