package com.browseengine.bobo.util.test;

import java.util.HashMap;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.FilterAtomicReader;
import org.apache.lucene.index.Term;

import com.browseengine.bobo.search.section.IntMetaDataCache;
import com.browseengine.bobo.search.section.MetaDataCache;
import com.browseengine.bobo.search.section.MetaDataCacheProvider;

public class IndexReaderWithMetaDataCache extends FilterAtomicReader implements MetaDataCacheProvider
{

  private final static Term intMetaTerm = new Term("metafield", "intmeta");
  private HashMap<Term,MetaDataCache> map = new HashMap<Term,MetaDataCache>();
  
  public IndexReaderWithMetaDataCache(AtomicReader in) throws Exception
  {
    super(in);
    
    map.put(intMetaTerm, new IntMetaDataCache(intMetaTerm, in));
  }

  public MetaDataCache get(Term term)
  {
    return map.get(term);
  }
}
