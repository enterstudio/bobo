package com.browseengine.bobo.api.codec;

import java.io.IOException;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

public class BoboDocValuesFormat extends DocValuesFormat {

  static final String NAME = "BoboDocValuesFormat";
  private final DocValuesFormat delegate;
  protected BoboDocValuesFormat(DocValuesFormat delegate) {
    super(NAME);
    this.delegate = delegate;
  }

  @Override
  public DocValuesConsumer fieldsConsumer(SegmentWriteState state)
      throws IOException {
    return new BoboDocValuesConsumer(state, delegate.fieldsConsumer(state));
  }

  @Override
  public DocValuesProducer fieldsProducer(SegmentReadState state)
      throws IOException {
    return new BoboDocValuesProducer(state, delegate.fieldsProducer(state));
  }

}
