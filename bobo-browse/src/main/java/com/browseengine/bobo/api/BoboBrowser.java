/**
 * 
 */
package com.browseengine.bobo.api;

import java.io.IOException;
import java.util.Set;

import com.browseengine.bobo.facets.FacetHandler;

/**
 * @author ymatsuda
 *
 */
public class BoboBrowser extends MultiBoboBrowser
{
  /**
   * @param reader BoboIndexReader
   * @throws IOException
   */
  public BoboBrowser(BoboCompositeReader reader) throws IOException
  {
    super(createBrowsables(reader));
  }
  
  public static BoboSubBrowser[] createSegmentedBrowsables(BoboIndexReader[] readerList){
	  BoboSubBrowser[] browsables = new BoboSubBrowser[readerList.length];
	  int i = 0;
	  for (BoboIndexReader reader : readerList){
		  browsables[i] = new BoboSubBrowser(reader);
		  i++;
	  }
	  return browsables;
  }

  public static BoboSubBrowser[] createBrowsables(BoboCompositeReader reader)
  {
    BoboIndexReader[] readerList = reader.getSubreaders();
    return createSegmentedBrowsables(readerList);
  }
  
  /**
   * Gets a set of facet names
   * 
   * @return set of facet names
   */
  public Set<String> getFacetNames()
  {
    return _subBrowsers[0].getFacetNames();
  }
  
  public FacetHandler<?> getFacetHandler(String name)
  {
    return _subBrowsers[0].getFacetHandler(name);
  }
}
