package com.browseengine.bobo.api.codec;

import java.io.IOException;

import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.OpenBitSet;

public class OpenBitsetDocIdSetCollector extends DocIdSetCollector {

  private OpenBitSet bs;
  public OpenBitsetDocIdSetCollector() {
    bs = null;
    
  }

  @Override
  void reset(int maxDoc) {
    if (bs == null) {
      bs = new OpenBitSet(maxDoc);
    }
    else {
      bs.clear(0, maxDoc);
    }
  }

  @Override
  void collect(int docid) {
    bs.set(docid);
  }

  @Override
  void flush(TermStats termStats, IndexOutput out) throws IOException {
    long[] bits = bs.getBits();
    out.writeVInt(bs.getNumWords());
    out.writeVInt(bits.length);
    for (long v : bits) {
      out.writeLong(v);
    }
  }

  @Override
  DocIdSet load(IndexInput in) throws IOException {
    int numWords = in.readVInt();
    int len = in.readVInt();
    long[] higherBitsArr = new long[len];
    for (int i = 0; i < len; ++i) {
      higherBitsArr[i] = in.readLong();
    }
    return new OpenBitSet(higherBitsArr, numWords);
  }

}
