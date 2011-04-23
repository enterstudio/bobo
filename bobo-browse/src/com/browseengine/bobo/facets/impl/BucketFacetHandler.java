package com.browseengine.bobo.facets.impl;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.facets.FacetCountCollector;
import com.browseengine.bobo.facets.FacetCountCollectorSource;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.FacetHandler.FacetDataNone;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.filter.EmptyFilter;
import com.browseengine.bobo.facets.filter.RandomAccessAndFilter;
import com.browseengine.bobo.facets.filter.RandomAccessFilter;
import com.browseengine.bobo.facets.filter.RandomAccessNotFilter;
import com.browseengine.bobo.sort.DocComparatorSource;

public class BucketFacetHandler extends FacetHandler<FacetDataNone>{
  private static Logger logger = Logger.getLogger(BucketFacetHandler.class);
  private final Map<String,String[]> _predefinedBuckets;
  private final String _dependOnFacetName;
  private FacetHandler<FacetDataCache<?>> _dependOnFacetHandler;
  String _name;
  
  public BucketFacetHandler(String name, Map<String,String[]> predefinedBuckets, String dependsOnFacetName)
  {
    super(name, new HashSet<String>(Arrays.asList(new String[]{dependsOnFacetName})));
    _name = name;
    _predefinedBuckets = predefinedBuckets;
    _dependOnFacetName  = dependsOnFacetName;
    _dependOnFacetHandler = null;
  }
  

  
  @Override
  public DocComparatorSource getDocComparatorSource() {
    return _dependOnFacetHandler.getDocComparatorSource();
  }
  
  @Override
  public String[] getFieldValues(BoboIndexReader reader, int id) {
    return _dependOnFacetHandler.getFieldValues(reader, id);
  }
  
  @Override
  public Object[] getRawFieldValues(BoboIndexReader reader,int id){
    return _dependOnFacetHandler.getRawFieldValues(reader, id);
  }

  @Override
  public RandomAccessFilter buildRandomAccessFilter(String bucketString, Properties prop) throws IOException
  {
      String[] elems = _predefinedBuckets.get(bucketString);
  
      if(elems == null || elems.length==0) return  EmptyFilter.getInstance();
      if(elems.length == 1) return _dependOnFacetHandler.buildRandomAccessFilter(elems[0], prop);
      return _dependOnFacetHandler.buildRandomAccessOrFilter(elems, prop, false);
  }
  
  
  @Override
  public RandomAccessFilter buildRandomAccessAndFilter(String[] bucketStrings,Properties prop) throws IOException
  {
    List<RandomAccessFilter> filterList = new LinkedList<RandomAccessFilter>();
    for (String bucketString : bucketStrings){
    	String[] vals = _predefinedBuckets.get(bucketString);
    	RandomAccessFilter filter = _dependOnFacetHandler.buildRandomAccessOrFilter(vals, prop, false);
    	if (filter==EmptyFilter.getInstance()) return EmptyFilter.getInstance();
    	filterList.add(filter);
    }
    if (filterList.size()==0) return EmptyFilter.getInstance();
    if (filterList.size()==1) return filterList.get(0);
    return new RandomAccessAndFilter(filterList);
  }

  @Override
  public RandomAccessFilter buildRandomAccessOrFilter(String[] bucketStrings,Properties prop,boolean isNot) throws IOException
  {
	if (isNot){
		RandomAccessFilter excludeFilter = buildRandomAccessAndFilter(bucketStrings, prop);
		return new RandomAccessNotFilter(excludeFilter);
	}
	else{
		Set<String> selections = new HashSet<String>();
		for (String bucket : bucketStrings){
			String[] vals = _predefinedBuckets.get(bucket);
			if (vals!=null){
				for (String val : vals){
				  selections.add(val);
				}
			}
		}
		if (selections!=null && selections.size()>0){
			String[] sels = selections.toArray(new String[0]);
			if (selections.size()==1){
			  return _dependOnFacetHandler.buildRandomAccessFilter(sels[0], prop);
			}
			else{
			  return _dependOnFacetHandler.buildRandomAccessOrFilter(sels, prop, false);
			}
		}
		else{
			return EmptyFilter.getInstance();
		}
	}
  }

  @Override
  public FacetCountCollectorSource getFacetCountCollectorSource(final BrowseSelection sel, final FacetSpec ospec) 
  {
    return new FacetCountCollectorSource() 
    {
      @Override
      public FacetCountCollector getFacetCountCollector(BoboIndexReader reader, int docBase){
    	DefaultFacetCountCollector countCollector = new DefaultFacetCountCollector() {
			
			@Override
			public void collectAll() {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void collect(int docid) {
				// TODO Auto-generated method stub
				
			}
		};
        return new BucketFacetCountCollector(_name, _dependOnFacetHandler.getFacetCountCollectorSource(sel, ospec).getFacetCountCollector(reader, docBase), ospec, _predefinedBuckets);
      }
    };

  }
  
  public boolean hasPredefinedBuckets()
  {
    return (_predefinedBuckets != null);
  }
  
  @Override
  public FacetDataNone load(BoboIndexReader reader) throws IOException
  {
	try{
      _dependOnFacetHandler = (FacetHandler<FacetDataCache<?>>)this.getDependedFacetHandler(_dependOnFacetName);
      FacetDataCache<?> dataCache = _dependOnFacetHandler.getFacetData(reader);
      
	}
	catch(Exception e){
	  throw new IOException("unable to load dependent facet handler: "+_dependOnFacetName);
	}
    return FacetDataNone.instance;
  } 
}

