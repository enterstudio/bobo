/**
 * 
 */
package com.browseengine.bobo.search.section;

import java.io.IOException;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;

/**
 * An abstract class for terminal nodes of SectionSearchQueryPlan
 */
public abstract class AbstractTerminalNode extends SectionSearchQueryPlan
{
  protected DocsAndPositionsEnum _tp;
  protected int _posLeft;
  protected int _curPos;
  
  public AbstractTerminalNode(Term term, AtomicReader reader) throws IOException
  {
    super();
    reader.termPositionsEnum(term);
    _posLeft = 0;
  }
  
  @Override
  public int fetchDoc(int targetDoc) throws IOException
  {
    if(targetDoc <= _curDoc) targetDoc = _curDoc + 1;
    
    int _curDoc;
    if((_curDoc = _tp.advance(targetDoc)) != DocIdSetIterator.NO_MORE_DOCS)
    {
      _posLeft = _tp.freq();
      _curSec = -1;
      _curPos = -1;
      return _curDoc;
    }
    else
    {
      _curDoc = DocIdSetIterator.NO_MORE_DOCS;
      return _curDoc;
    }
  }
  
  abstract public int fetchSec(int targetSec) throws IOException;
}
