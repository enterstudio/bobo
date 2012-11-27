/**
 * 
 */
package com.browseengine.bobo.search.section;

import java.io.IOException;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;

/**
 *
 */
public class SectionSearchQuery extends Query
{ 
  private Query _query;
  
  private class SectionSearchWeight extends Weight
  {
    
    float _weight;

    public String toString()
    {
      return "weight(" + SectionSearchQuery.this + ")";
    }

    public Query getQuery()
    {
      return SectionSearchQuery.this;
    }

    public Scorer scorer(AtomicReader reader,Bits accepted) throws IOException
    {
      SectionSearchScorer scorer = new SectionSearchScorer(this, getValueForNormalization(), reader);
      
      return scorer;
    }

    @Override
    public Explanation explain(AtomicReaderContext readerCtx, int doc) throws IOException
    {
      Explanation result = new Explanation();
      result.setValue(_weight);
      result.setDescription(SectionSearchQuery.this.toString());

      return result;
    }

    @Override
    public Scorer scorer(AtomicReaderContext context, boolean scoreDocsInOrder,
        boolean topScorer, Bits acceptDocs) throws IOException
    {
      AtomicReader reader = context.reader();
      return scorer(reader,acceptDocs);
    }

    @Override
    public float getValueForNormalization() throws IOException {
      return getBoost();
    }

    @Override
    public void normalize(float queryNorm, float topLevelBoost) {
      _weight *= queryNorm * topLevelBoost;
    }
  }

  public class SectionSearchScorer extends Scorer
  {
    private int              _curDoc = -1;
    private float            _curScr;
    private boolean          _more = true; // more hits
    private SectionSearchQueryPlan _plan;
    
    public SectionSearchScorer(Weight weight, float score, AtomicReader reader)
      throws IOException
    {
      super(weight);
      _curScr = score;
      
      SectionSearchQueryPlanBuilder builer = new SectionSearchQueryPlanBuilder(reader);
      _plan = builer.getPlan(_query);
      if(_plan != null)
      {
        _curDoc = -1;
        _more = true;
      }
      else
      {
        _curDoc = DocIdSetIterator.NO_MORE_DOCS;
        _more = false;;        
      }
    }
    
    @Override
    public int docID()
    {
      return _curDoc;
    }

    @Override
    public int nextDoc() throws IOException
    {
      return advance(0);
    }

    @Override
    public float score() throws IOException
    {
      return _curScr;
    }

    @Override
    public int advance(int target) throws IOException
    {
      if(_curDoc < DocIdSetIterator.NO_MORE_DOCS)
      {
        if(target <= _curDoc) target = _curDoc + 1;
  
        return _plan.fetch(target);
      }
      return _curDoc;
    }

    @Override
    public float freq() throws IOException {
      return 1;
    }
  }
  
  /**
   * constructs SectionSearchQuery
   * 
   * @param query
   */
  public SectionSearchQuery(Query query)
  {
    _query = query;
  }

  @Override
  public String toString(String field)
  {
    StringBuilder buffer = new StringBuilder();
    buffer.append("SECTION(" + _query.toString() + ")");
    return buffer.toString();
  }

  @Override
  public Weight createWeight(IndexSearcher searcher) throws IOException
  {
    return new SectionSearchWeight();
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException
  {
    _query.rewrite(reader);
    return this;
  }
}
