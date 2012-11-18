package com.browseengine.bobo.api;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;

import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.FacetHandlerInitializerParam;
import com.browseengine.bobo.facets.RuntimeFacetHandler;
import com.browseengine.bobo.facets.RuntimeFacetHandlerFactory;

/**
 * Per-search runtime facet context
 */
public class RuntimeFacetContext implements Closeable {
  
  private static Logger logger = Logger.getLogger(RuntimeFacetContext.class);
  
  private final Map<String,RuntimeFacetHandlerFactory<FacetHandlerInitializerParam,?>> runtimeFacetHandlerFactoryMap;
  
  final Map<String,RuntimeFacetHandler<?>> runtimeFacetHandlerMap;
  
  public RuntimeFacetContext(Map<String,RuntimeFacetHandlerFactory<FacetHandlerInitializerParam,?>> runtimeFacetHandlerFactoryMap){
    this.runtimeFacetHandlerFactoryMap = runtimeFacetHandlerFactoryMap;
    this.runtimeFacetHandlerMap = new HashMap<String,RuntimeFacetHandler<?>>();
  }
  
  public void loadFacetHandler(String facetName, FacetHandlerInitializerParam data, BoboCompositeReader reader) throws IOException{
    if (runtimeFacetHandlerMap.containsKey(facetName)){
      logger.warn("attempting to reset facetHandler: " + facetName);
      return;
    }
    
    RuntimeFacetHandlerFactory<FacetHandlerInitializerParam,?> factory = runtimeFacetHandlerFactoryMap.get(facetName);

    if (data == null)
      data = FacetHandlerInitializerParam.EMPTY_PARAM;
    if (data != FacetHandlerInitializerParam.EMPTY_PARAM || !factory.isLoadLazily())
    {
      RuntimeFacetHandler<?> facetHandler =  factory.get(data);
      if (facetHandler != null)
      {
        Set<String> dependsOn = facetHandler.getDependsOn();
        if (dependsOn.size() > 0)
        {
          Iterator<String> iter = dependsOn.iterator();
          while(iter.hasNext())
          {
            String fn = iter.next();
            // see if it is a runtime facet handler
            FacetHandler<?> f = runtimeFacetHandlerMap.get(fn);
            if (f == null)
            {
              // see if it is a static facet handler
              f = reader.getFacetHandler(fn);
            }
            if (f==null)
            {
              throw new IOException("depended on facet handler: "+fn+", but is not found");
            }
            facetHandler.putDependedFacetHandler(f);
          }
        }

        BoboIndexReader[] subreaders = reader.getSubreaders();
        for (BoboIndexReader subreader : subreaders){
          facetHandler.load(subreader);
        }
        runtimeFacetHandlerMap.put(facetName,facetHandler); // add to a list so we close them after search

      }
    } 
  }

  @Override
  public void close() throws IOException {
    Set<Entry<String,RuntimeFacetHandler<?>>> entries = runtimeFacetHandlerMap.entrySet();
    for (Entry<String,RuntimeFacetHandler<?>> entry : entries){
      entry.getValue().close();
    }
    runtimeFacetHandlerMap.clear();
  }

}
