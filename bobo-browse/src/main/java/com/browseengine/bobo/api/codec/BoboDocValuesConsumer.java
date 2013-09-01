package com.browseengine.bobo.api.codec;

import java.io.IOException;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.BytesRef;

public class BoboDocValuesConsumer extends DocValuesConsumer {

  private final SegmentWriteState state;
  private final DocValuesConsumer delegate;
  
  public BoboDocValuesConsumer(SegmentWriteState state, DocValuesConsumer delegate) {
    this.state = state;
    this.delegate = delegate;
  }
  
  @Override
  public void close() throws IOException {
    delegate.close();
  }

  @Override
  public void addNumericField(FieldInfo field, Iterable<Number> values)
      throws IOException {
    int numDocs = state.segmentInfo.getDocCount();
    long[] vals = new long[numDocs];
    int i = 0;
    for (Number value : values) {
      vals[i++] = value.longValue();
    }
    delegate.addNumericField(field, values);
  }

  @Override
  public void addBinaryField(FieldInfo field, Iterable<BytesRef> values)
      throws IOException {
    delegate.addBinaryField(field, values);
  }

  @Override
  public void addSortedField(FieldInfo field, Iterable<BytesRef> values,
      Iterable<Number> docToOrd) throws IOException {
    delegate.addSortedField(field, values, docToOrd);
  }

  @Override
  public void addSortedSetField(FieldInfo field, Iterable<BytesRef> values,
      Iterable<Number> docToOrdCount, Iterable<Number> ords) throws IOException {
    delegate.addSortedSetField(field, values, docToOrdCount, ords);

  }

}
