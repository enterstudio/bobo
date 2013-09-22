package com.browseengine.bobo.api.codec;

import java.io.IOException;

import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

public abstract class DocIdSetCollector {
  abstract void reset(int maxDoc);
  abstract void collect(int docid);
  abstract void flush(TermStats termStats, IndexOutput out) throws IOException;
  
  abstract DocIdSet load(IndexInput in) throws IOException;
}
