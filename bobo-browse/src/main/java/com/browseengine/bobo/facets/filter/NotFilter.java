package com.browseengine.bobo.facets.filter;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;

import com.kamikaze.docidset.impl.NotDocIdSet;

public class NotFilter extends Filter {
  
	private final Filter _innerFilter;
	
	public NotFilter(Filter innerFilter)
	{
		_innerFilter = innerFilter;
	}
	@Override
	public DocIdSet getDocIdSet(AtomicReaderContext readerCtx, Bits liveDocs) throws IOException {
	  int maxDoc = readerCtx.reader().maxDoc();
		return new NotDocIdSet(_innerFilter.getDocIdSet(readerCtx,liveDocs),maxDoc);
	}
	
}

