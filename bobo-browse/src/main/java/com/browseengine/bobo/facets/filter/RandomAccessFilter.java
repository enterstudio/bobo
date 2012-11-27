package com.browseengine.bobo.facets.filter;

import java.io.IOException;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.docidset.RandomAccessDocIdSet;

public abstract class RandomAccessFilter extends Filter
{
  @Override 
  public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException
  {
	AtomicReader reader = context.reader();
	if (reader instanceof BoboIndexReader){
      return getRandomAccessDocIdSet((BoboIndexReader)reader, acceptDocs);
	}
	else{
	  throw new IllegalStateException("reader not instance of "+BoboIndexReader.class);
	}
  }
  
  public abstract RandomAccessDocIdSet getRandomAccessDocIdSet(BoboIndexReader reader, Bits liveDocs) throws IOException;
  public double getFacetSelectivity(BoboIndexReader reader) { return 0.50; }
  
}
