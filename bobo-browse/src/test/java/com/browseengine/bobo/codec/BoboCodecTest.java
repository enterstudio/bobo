package com.browseengine.bobo.codec;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.util.Random;

import junit.framework.TestCase;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.BasePostingsFormatTestCase;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.junit.Test;

import com.browseengine.bobo.api.codec.BoboCodec;

public class BoboCodecTest {

  static LongSet gendata(int numUniques) {
    LongSet valSet = new LongOpenHashSet();
    Random rand = new Random();
    
    while(true) {
      long v = Math.abs(rand.nextLong());
      valSet.add(v);
      if (valSet.size() >= numUniques) break;
    }
    return valSet;
  }
  
  static void fill(long[] vals, LongSet uniqVals) {
    long[] uniqValArr = uniqVals.toLongArray();
    
    Random rand = new Random();
    
    for (int i = 0; i < vals.length; ++i) {
      int idx = rand.nextInt(uniqValArr.length);
      vals[i] = uniqValArr[idx];
    }
  }
  
  @Test
  public void testCodecBasic() throws Exception{
    IndexWriterConfig writerConfig = new IndexWriterConfig(Version.LUCENE_44, new StandardAnalyzer(Version.LUCENE_44));
    writerConfig.setCodec(new BoboCodec());
    LongSet vals = gendata(250);
    
    int numdocs = 1000;
    long[] docs = new long[numdocs];
    fill(docs, vals);
    
    RAMDirectory dir = new RAMDirectory();
    
    IndexWriter writer = new IndexWriter(dir, writerConfig);
    
    for (int i = 0; i < docs.length; ++i) {
      Document doc = new Document();
      NumericDocValuesField f = new NumericDocValuesField("num", docs[i]);
      doc.add(f);
      writer.addDocument(doc);
    }
    
    writer.commit();
    writer.close();
    
    IndexReader reader = DirectoryReader.open(dir);
    
    TestCase.assertEquals(docs.length, reader.numDocs());
    
    reader.close();
  }
  
  private static BoboCodec codec = new BoboCodec();
}
