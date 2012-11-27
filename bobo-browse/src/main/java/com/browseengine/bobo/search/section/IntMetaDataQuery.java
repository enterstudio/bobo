/**
 * 
 */
package com.browseengine.bobo.search.section;

import java.io.IOException;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;

/**
 *
 */
public class IntMetaDataQuery extends MetaDataQuery
{ 
  private Validator _validator;

  public static abstract class Validator
  {
    public abstract boolean validate(int datum);
  }
  
  public static class SimpleValueValidator extends Validator
  {
    private static final long serialVersionUID = 1L;
    
    private final int _val;
    
    public SimpleValueValidator(int val)
    {
      _val = val;
    }
    
    public boolean validate(int datum)
    {
      return (datum == _val);
    }
    
    public String toString()
    {
      return "SingleValueValidator[" + _val + "]";
    }
  }
  
  public static class SimpleRangeValidator extends Validator
  {
    private static final long serialVersionUID = 1L;
    
    private final int _lower;
    private final int _upper;
    
    public SimpleRangeValidator(int lower, int upper)
    {
      _lower = lower;
      _upper = upper;
    }
    
    public boolean validate(int datum)
    {
      return (datum >= _lower && datum <= _upper);
    }
    
    public String toString()
    {
      return "RangeValidator[" + _lower + "," + _upper + "]";
    }
  }
  
  /**
   * constructs IntMetaDataQueryQuery
   * 
   * @param term
   * @param validator
   */
  public IntMetaDataQuery(Term term, Validator validator)
  {
    super(term);
    _validator = validator;
  }

  @Override
  public String toString(String field)
  {
    return "IntMetaDataQuery(" + _validator.toString() + ")";
  }

  @Override
  public Weight createWeight(IndexSearcher searcher) throws IOException
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException
  {
    return this;
  }
  
  @Override
  public SectionSearchQueryPlan getPlan(AtomicReader reader) throws IOException
  {
    return new IntMetaDataNodeNoCache(_term, reader, _validator);
  }
  
  @Override
  public SectionSearchQueryPlan getPlan(MetaDataCache cache) throws IOException
  {
    return new IntMetaDataNode((IntMetaDataCache)cache, _validator);
  }
  
  public static class IntMetaDataNodeNoCache extends AbstractTerminalNode
  {
    private final Validator _validator;
    private BytesRef _data;
    private int _dataLen;

    public IntMetaDataNodeNoCache(Term term, AtomicReader reader, Validator validator)
      throws IOException
    {
      super(term, reader);
      _validator = validator;
    }
    
    @Override
    public int fetchDoc(int targetDoc) throws IOException
    {
      _dataLen = -1;
      return super.fetchDoc(targetDoc);
    }
    
    @Override
    public int fetchSec(int targetSec) throws IOException
    {
      if(_curSec == SectionSearchQueryPlan.NO_MORE_SECTIONS) return _curSec;
      
      if(targetSec <= _curSec) targetSec = _curSec + 1;

   
      if(_dataLen == -1 && _posLeft > 0)
      {
        _tp.nextPosition();
        BytesRef payload = _tp.getPayload();
        if(payload != null)
        {
          _dataLen = payload.length;
          _data = payload;
        }
      }
      int offset = targetSec * 4;
      byte[] bytes = _data.bytes;
      while(offset + 4 <= _dataLen)
      {
        int datum = ((bytes[_data.offset+offset] & 0xff) |
                     ((bytes[_data.offset+offset + 1] & 0xff) << 8) |
                     ((bytes[_data.offset+offset + 2] & 0xff) << 16) |
                     ((bytes[_data.offset+offset + 3] & 0xff) << 24));
        
        if(_validator.validate(datum))
        {
          _curSec = targetSec;
          return _curSec;
        }
        targetSec++;
        offset = targetSec * 4;
      }
      _curSec = SectionSearchQueryPlan.NO_MORE_SECTIONS;
      return _curSec;
    }
  }
  
  static public class IntMetaDataNode extends SectionSearchQueryPlan
  {
    private final IntMetaDataCache _cache;
    private final Validator _validator;
    private final int _maxDoc;
    
    private int _maxSec;
    
    public IntMetaDataNode(IntMetaDataCache cache, Validator validator)
      throws IOException
    {
      super();
      _cache = cache;
      _maxDoc = cache.maxDoc();
      _validator = validator;
    }
    
    @Override
    public int fetchDoc(int targetDoc) throws IOException
    {
      if(_curDoc == DocIdSetIterator.NO_MORE_DOCS) return _curDoc;
      
      if(targetDoc <= _curDoc) targetDoc = _curDoc + 1;
      
      _curSec = -1;

      while(targetDoc <_maxDoc)
      {
        _maxSec = _cache.getNumItems(targetDoc);
        
        if(_maxSec <= 0)
        {
          targetDoc++;
          continue;
        }
        _curDoc = targetDoc;
        return _curDoc;
      }
      _curDoc = DocIdSetIterator.NO_MORE_DOCS;
      return _curDoc;
    }
    
    @Override
    public int fetchSec(int targetSec) throws IOException
    {
      if(_curSec == SectionSearchQueryPlan.NO_MORE_SECTIONS) return _curSec;
      
      if(targetSec <= _curSec) targetSec = _curSec + 1;
      
      while(targetSec < _maxSec)
      {
        int datum = _cache.getValue(_curDoc, targetSec, 0);
        
        if(_validator.validate(datum))
        {
          _curSec = targetSec;
          return _curSec;
        }
        targetSec++;
      }
      _curSec = SectionSearchQueryPlan.NO_MORE_SECTIONS;
      return _curSec;
    }
  }
}
