package com.browseengine.bobo.api.codec;

import java.io.IOException;
import java.util.Comparator;

import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;

import com.browseengine.bobo.api.codec.BoboFacetFieldsProducer.FieldMeta;
import com.browseengine.bobo.api.codec.BoboFacetFieldsProducer.TermMetaWithState;

public class BoboFacetHashTerms extends Terms{

  private final TermMetaWithState[] termMetas;
  private final FieldMeta fieldMeta;
  private final PostingsReaderBase postingsReader;
  private final Comparator<BytesRef> comparator;
  private final BytesRef[] terms;
  
  BoboFacetHashTerms(BytesRef[] terms, TermMetaWithState[] termMetas, FieldMeta fieldMeta, PostingsReaderBase postingsReader, Comparator<BytesRef> comparator) {
    this.terms = terms;
    this.termMetas = termMetas;
    this.fieldMeta = fieldMeta;
    this.comparator = comparator;
    this.postingsReader = postingsReader;
  }
  
  @Override
  public TermsEnum iterator(TermsEnum reuse) throws IOException {
    
    //FieldInfo finfo, BytesRef[] terms, TermMetaWithState[] termMetas, PostingsReaderBase postingsReader, final Comparator<BytesRef> comparator
    return new BoboFacetTermsEnum(fieldMeta.field, terms, termMetas, postingsReader, comparator);
  }

  @Override
  public Comparator<BytesRef> getComparator() {
    return comparator;
  }

  @Override
  public long size() throws IOException {
    return termMetas.length;
  }

  @Override
  public long getSumTotalTermFreq() throws IOException {
    return fieldMeta.sumTotalTermFreq;
  }

  @Override
  public long getSumDocFreq() throws IOException {
    return fieldMeta.sumDocFreq;
  }

  @Override
  public int getDocCount() throws IOException {
    return fieldMeta.docCount;
  }

  @Override
  public boolean hasOffsets() {
    return fieldMeta.field.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
  }

  @Override
  public boolean hasPositions() {
    return fieldMeta.field.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
  }

  @Override
  public boolean hasPayloads() {
    return fieldMeta.field.hasPayloads();
  }

}
