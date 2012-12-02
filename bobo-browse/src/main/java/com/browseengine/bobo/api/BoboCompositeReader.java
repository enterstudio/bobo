package com.browseengine.bobo.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.BaseCompositeReader;
import org.apache.lucene.index.IndexReader;

import com.browseengine.bobo.api.BoboIndexReader.WorkArea;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.FacetHandlerInitializerParam;
import com.browseengine.bobo.facets.RuntimeFacetHandlerFactory;

public class BoboCompositeReader extends BaseCompositeReader<BoboIndexReader>{

  private final boolean closeSubreaders;
  private BoboIndexReader[] subreaderArray = null;
  Map<String,FacetHandler<?>> facetHandlerMap;

  Map<String,RuntimeFacetHandlerFactory<FacetHandlerInitializerParam,?>> runtimeFacetHandlerFactoryMap;
  
  public BoboCompositeReader(BoboIndexReader[] subReaders, boolean closeSubReaders) {
    super(subReaders);
    this.closeSubreaders = closeSubReaders;
    
    if (!closeSubReaders) {
      for (int i = 0; i < subReaders.length; i++) {
        facetHandlerMap = subReaders[i]._facetHandlerMap;
        runtimeFacetHandlerFactoryMap = subReaders[i]._runtimeFacetHandlerFactoryMap;
        subReaders[i].incRef();
      }
    }
  }
  
  public BoboCompositeReader(BoboIndexReader[] subReaders){
    this(subReaders,true);
  }
  
  public BoboCompositeReader(IndexReader reader, Collection<FacetHandler<?>> facetHandlers) throws IOException{
    this(reader,facetHandlers,null,true);
  }
  
  public BoboCompositeReader(IndexReader reader, Collection<FacetHandler<?>> facetHandlers,
      Collection<RuntimeFacetHandlerFactory<FacetHandlerInitializerParam,?>> facetHandlerFactories) throws IOException{
    this(reader,facetHandlers,facetHandlerFactories,true);
  }
  
  public BoboCompositeReader(IndexReader reader,Collection<FacetHandler<?>> facetHandlers,
      Collection<RuntimeFacetHandlerFactory<FacetHandlerInitializerParam,?>> facetHandlerFactories, boolean closeSubReaders) throws IOException{
    this(extractLeafReaders(reader,facetHandlers,facetHandlerFactories).toArray(new BoboIndexReader[0]),closeSubReaders);
  }
  
  private static ArrayList<BoboIndexReader> extractLeafReaders(IndexReader r,Collection<FacetHandler<?>> facetHandlers,
      Collection<RuntimeFacetHandlerFactory<FacetHandlerInitializerParam,?>> facetHandlerFactories) throws IOException{
    List<AtomicReaderContext> leaves = r.getContext().leaves();
    ArrayList<BoboIndexReader> readerList = new ArrayList<BoboIndexReader>(leaves.size());
    for (AtomicReaderContext leaf : leaves){
      readerList.add(new BoboIndexReader(leaf.reader(),facetHandlers,facetHandlerFactories,new WorkArea()));
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
  
  public FacetHandler<?> getFacetHandler(String name){
    return facetHandlerMap.get(name);
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
