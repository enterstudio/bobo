package com.browseengine.bobo.facets.statistics;

import java.util.Arrays;

import com.browseengine.bobo.facets.FacetCountCollector;

public abstract class FacetCountStatisicsGenerator
{
  private int _minCount = 1;
  
  public int getMinCount()
  {
    return _minCount;
  }

  public void setMinCount(int minCount)
  {
    _minCount = minCount;
  }
  
  public abstract double calculateDistributionScore(int[] distribution,int collectedSampleCount,int numSamplesCollected,int totalSamplesCount);

  public FacetCountStatistics generateStatistic(int[] distribution,int n)
  {
    int[] tmp=distribution;
    int totalSampleCount=distribution.length;
    boolean sorted=false;
    if (n>0)
    {
      totalSampleCount = Math.min(n, tmp.length);
      // this is crappy, to be made better with a pq
      int[] tmp2 = new int[distribution.length];
      System.arraycopy(distribution, 0, tmp2, 0, distribution.length);
      
      Arrays.sort(tmp2);
      
      tmp = new int[totalSampleCount];
      System.arraycopy(tmp2, 0, tmp, 0, tmp.length);
      sorted = true;
    }
    
    int collectedSampleCount = 0;
    int numSamplesCollected = 0;
    
    for (int count : tmp)
    {
      if (count >= _minCount)
      {
        collectedSampleCount+=count; 
        numSamplesCollected++;
      }
      else
      {
        if (sorted) break;
      }
    }
    
    double distScore = calculateDistributionScore(tmp, collectedSampleCount, numSamplesCollected,totalSampleCount);
    
    FacetCountStatistics stats = new FacetCountStatistics();
    
    stats.setDistribution(distScore);
    stats.setNumSamplesCollected(numSamplesCollected);
    stats.setCollectedSampleCount(collectedSampleCount);
    stats.setTotalSampleCount(totalSampleCount);
    return stats;
  }
  
  public FacetCountStatistics generateStatistic(FacetCountCollector countHitCollector,int n)
  {
    return generateStatistic(countHitCollector.getCountDistribution(),n);
  } 
}
