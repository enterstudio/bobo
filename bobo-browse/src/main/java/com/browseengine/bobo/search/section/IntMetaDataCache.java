/**
 * 
 */
package com.browseengine.bobo.search.section;

import java.io.IOException;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;

/**
 *
 */
public class IntMetaDataCache implements MetaDataCache
{
  private static final int MAX_SLOTS = 1024; 
  private static final int MISSING = Integer.MIN_VALUE;
  
  private final AtomicReader _reader;
  private int[][] _list;
  
  private int _curPageNo;
  private int[] _curPage;
  private int _curSlot;
  private int _curData;

  public IntMetaDataCache(Term term, AtomicReader reader) throws IOException
  {
    _reader = reader;
    
    int maxDoc = reader.maxDoc();
    _list = new int[(maxDoc + MAX_SLOTS - 1) / MAX_SLOTS][];
    _curPageNo = 0;
    _curSlot = 0;
    _curData = MAX_SLOTS;
    
    if(maxDoc > 0)
    {
      _curPage = new int[MAX_SLOTS * 2];
      loadPayload(term);
    }
    
    _curPage = null;
  }
  
  protected void add(int docid, BytesRef data)
  {
    int pageNo = docid / MAX_SLOTS;
    if(pageNo != _curPageNo)
    {
      // save the page
      
      while (_curSlot < MAX_SLOTS)
      {
        _curPage[_curSlot++] = MISSING;
      }
      _list[_curPageNo++] = copyPage(new int[_curData]);  // optimize the page to make getMaxItems work
      _curSlot = 0;
      _curData = MAX_SLOTS;

      while (_curPageNo < pageNo)
      {
        _list[_curPageNo++] = null;
      }
    }
    
    while (_curSlot < docid % MAX_SLOTS)
    {
      _curPage[_curSlot++] = MISSING;
    }

    if(data.length <= 4)
    {
      int val = 0;
      if(data.length == 0)
      {
        val = MISSING;
      }
      else
      {
        byte[] bytes = data.bytes;
        for(int i = 0; i < 4; i++)
        {
          if(i >= data.length) break;
          
          val |= ((bytes[i+data.offset] & 0xff) << (i * 8));
        }
      }
      if (val >= 0) 
      {
        _curPage[_curSlot] = val;
      }
      else
      {
        appendToTail(data);
      }
    }
    else
    {
      appendToTail(data);
    }
    _curSlot++;
  }
  
  private void appendToTail(BytesRef data)
  {
    int ilen = (data.length + 3) / 4; // length in ints
    
    if(_curPage.length <= _curData + ilen)
    {
      // double the size of the variable part at least
      _curPage = copyPage(new int[_curPage.length + Math.max((_curPage.length - MAX_SLOTS), ilen)]);
    }
    _curPage[_curSlot] = (- _curData);
    _curData = copyByteToInt(data, _curPage, _curData);
  }
  
  private int copyByteToInt(BytesRef data, int[] dst, int dstoff)
  {
    int blen = data.length;
    int off = data.offset;
    while(blen > 0)
    {
      int val = 0;
      for(int i = 0; i < 4; i++)
      {
        blen--;
        
        if(off >= blen) break; // may not have all bytes            
        val |= ((data.bytes[off++] & 0xff) << (i * 8));
      }

      dst[dstoff++] = val;
    }
    return dstoff;
  }
  
  private int[] copyPage(int[] dst)
  {
    System.arraycopy(_curPage, 0, dst, 0, _curData);
    return dst;
  }

  protected void loadPayload(Term term) throws IOException
  {
    DocsAndPositionsEnum tp = _reader.termPositionsEnum(term);
    
    int doc;
    while((doc = tp.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS)
    {
      if(tp.freq() > 0)
      {
        tp.nextPosition();
        BytesRef payload = tp.getPayload();
        if(payload != null)
        {
          add(doc, payload);
        }
      }
    }
    
    // save the last page
    
    while (_curSlot < MAX_SLOTS)
    {
      _curPage[_curSlot++] = MISSING;
    }
    _list[_curPageNo] = copyPage(new int[_curData]); // optimize the page to make getNumItems work
    _curPage = null;
  }

  public int getValue(int docid, int idx, int defaultValue)
  {
    int[] page = _list[docid / MAX_SLOTS];
    if(page == null) return defaultValue;
    
    int val = page[docid % MAX_SLOTS];
    if (val >= 0)
    {
      return val;
    }
    else
    {
      return (val == MISSING ?  defaultValue : page[idx - val]);
    }
  }
  
  public int getNumItems(int docid)
  {
    int[] page = _list[docid / MAX_SLOTS];
    if(page == null) return 0;
    
    int slotNo = docid % MAX_SLOTS;
    int val = page[slotNo];
    
    if (val >= 0) return 1;
    
    if(val == MISSING) return 0;
    
    slotNo++;
    while(slotNo < MAX_SLOTS)
    {
      int nextVal = page[slotNo++];
      if (nextVal < 0 && nextVal != MISSING)
      {
        return (val - nextVal);
      }
    }
    return (val + page.length);
  }
  
  public int maxDoc()
  {
    return _reader.maxDoc();
  }
}
