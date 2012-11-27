package com.browseengine.bobo.query;

import java.io.IOException;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;

public class MatchAllDocIdSetIterator extends DocIdSetIterator {
  private int _docid;
  private final int maxDoc;
  private final Bits liveDocs;
	
  public MatchAllDocIdSetIterator(AtomicReader reader) throws IOException {
    maxDoc = reader.maxDoc();
    liveDocs = reader.getLiveDocs();
		_docid = -1;
	}
	@Override
	public int advance(int target) throws IOException {
	  _docid = target-1;
    return nextDoc();
	}
	
	@Override
	public int docID() {
		return _docid;
	}
	
	@Override
	public int nextDoc() throws IOException {
	  _docid++;
    while(liveDocs != null && _docid < maxDoc && !liveDocs.get(_docid)) {
      _docid++;
    }
    if (_docid == maxDoc) {
      _docid = NO_MORE_DOCS;
    }
    return _docid;
	}
}
