package com.browseengine.bobo.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.BaseCompositeReader;
import org.apache.lucene.index.IndexReader;

public class BoboCompositeReader extends BaseCompositeReader<BoboIndexReader>{

  private final boolean closeSubreaders;
  private BoboIndexReader[] subreaderArray = null;
  
  public BoboCompositeReader(BoboIndexReader[] subReaders, boolean closeSubReaders) {
    super(subReaders);
    this.closeSubreaders = closeSubReaders;
    if (!closeSubReaders) {
      for (int i = 0; i < subReaders.length; i++) {
        subReaders[i].incRef();
      }
    }
  }
  
  public BoboCompositeReader(BoboIndexReader[] subReaders){
    this(subReaders,true);
  }
  
  public BoboCompositeReader(IndexReader reader){
    this(reader,true);
  }
  
  public BoboCompositeReader(IndexReader reader,boolean closeSubReaders){
    this(extractLeafReaders(reader).toArray(new BoboIndexReader[0]),closeSubReaders);
  }
  
  private static ArrayList<BoboIndexReader> extractLeafReaders(IndexReader r) {
    List<AtomicReaderContext> leaves = r.getContext().leaves();
    ArrayList<BoboIndexReader> readerList = new ArrayList<BoboIndexReader>(leaves.size());
    for (AtomicReaderContext leaf : leaves){
          readerList.add(new BoboIndexReader(leaf.reader()));
    }
    return readerList;
  }
  
  public BoboIndexReader[] getSubreaders(){
    if (subreaderArray == null){
      subreaderArray = getSequentialSubReaders().toArray(new BoboIndexReader[0]);
    }
    return subreaderArray;
  }

  public BoboIndexReader getSubreader(int docid){
    int readerIdx = readerIndex(docid);
    return subreaderArray[readerIdx];
  }

  public int getBaseDocId(int docid){
    return readerBase(readerIndex(docid));
  }

  @Override
  protected void doClose() throws IOException {
    IOException ioe = null;
    for (final IndexReader r : getSequentialSubReaders()) {
      try {
        if (closeSubreaders) {
          r.close();
        } else {
          r.decRef();
        }
      } catch (IOException e) {
        if (ioe == null) ioe = e;
      }
    }
    // throw the first exception
    if (ioe != null) throw ioe;
  }

}
