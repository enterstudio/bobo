package com.browseengine.bobo.api.codec;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

import com.browseengine.bobo.api.codec.BoboFacetFieldsProducer.TermMetaWithState;

public class BoboFacetTermsEnum extends TermsEnum {
  private final BytesRef[] terms;
  private final TermMetaWithState[] termMetas;
  private final Comparator<BytesRef> comparator;
  private final PostingsReaderBase postingsReader;
  private final FieldInfo finfo;
  private int cursor = -1;
  
  BoboFacetTermsEnum(FieldInfo finfo, BytesRef[] terms, TermMetaWithState[] termMetas, PostingsReaderBase postingsReader, final Comparator<BytesRef> comparator) {
    this.finfo = finfo;
    this.terms = terms;
    this.termMetas = termMetas;
    this.postingsReader = postingsReader;
    this.comparator = comparator;
  }
  
  @Override
  public BytesRef next() throws IOException {
    cursor ++;
    if (cursor >= terms.length) return null;
    return terms[cursor];
  }

  @Override
  public Comparator<BytesRef> getComparator() {
    return comparator;
  }
  
  @Override
  public SeekStatus seekCeil(BytesRef text, boolean useCache)
      throws IOException {
    int idx = Arrays.binarySearch(terms, text, comparator);
    if (idx >= 0) {
      cursor = idx;
      return SeekStatus.FOUND;
    }
    else {
      idx = -(idx + 1);
      if (idx < terms.length) {
        cursor = idx;
        return SeekStatus.NOT_FOUND;
      }
      else {
        cursor = -1;
        return SeekStatus.END;
      }
    }
  }

  @Override
  public void seekExact(long ord) throws IOException {
    cursor = (int)ord;
  }

  @Override
  public BytesRef term() throws IOException {
    return terms[cursor];
  }

  @Override
  public long ord() throws IOException {
    return cursor;
  }

  @Override
  public int docFreq() throws IOException {
    return termMetas[cursor].df;
  }

  @Override
  public long totalTermFreq() throws IOException {
    return termMetas[cursor].totalTf;
  }
  
  @Override
  public TermState termState() throws IOException {
    return termMetas[cursor].termState.clone();
  }

  @Override
  public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags)
      throws IOException {
    return postingsReader.docs(finfo, termMetas[cursor].termState, liveDocs, reuse, flags);
  }

  @Override
  public DocsAndPositionsEnum docsAndPositions(Bits liveDocs,
      DocsAndPositionsEnum reuse, int flags) throws IOException {
    return postingsReader.docsAndPositions(finfo, termMetas[cursor].termState, liveDocs, reuse, flags);
  }

}
