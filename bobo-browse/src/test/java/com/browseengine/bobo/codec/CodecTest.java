package com.browseengine.bobo.codec;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene42.Lucene42Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;

import com.browseengine.bobo.api.codec.BoboFacetPostingsFormat;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class CodecTest {
  static final String FIELD = "test";
  static Directory buildIndex(Iterable<String> datasrc, Codec codec) throws Exception{
    String idxname = codec == null ? "lucene" : codec.getName();
    Directory dir = FSDirectory.open(new File("/tmp/codectest",idxname));//new RAMDirectory();
    //Directory dir = new RAMDirectory();
    IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_44, new StandardAnalyzer(Version.LUCENE_44));
    conf.setUseCompoundFile(false);
    if (codec != null) {
      conf.setCodec(codec);
    }
    
    IndexWriter writer = new IndexWriter(dir, conf);
    
    for (String doc : datasrc) {
      if (doc == null) break;
      doc = doc.trim();
      if (doc.length() == 0) continue;
      Document d = new Document();
      FieldType ft = new FieldType();
      ft.setIndexed(true);
      ft.setStored(false);
      ft.setIndexOptions(IndexOptions.DOCS_ONLY);
      ft.setOmitNorms(true);
      Field f = new Field(FIELD,doc,ft);
      d.add(f);
      writer.addDocument(d);
    }
    writer.forceMerge(1);
    writer.commit();
    writer.close();
    return dir;
  }
  
  static Iterable<String> buildDataSrc(File f) throws IOException{
    final int maxCount = 1000;
    final BufferedReader freader = new BufferedReader(new FileReader(f));
    ArrayList<String> list = new ArrayList<String>(maxCount);
    
    while(list.size() < maxCount) {
      String line = freader.readLine();
      if (line == null) break;
      
      if (line.trim().length() > 0) {
        list.add(line);
      }
    }
    
    freader.close();
    
    return list;
  }
  
  
  static void testThreaded(int numThreads,final int numIter, final AtomicReader reader, final String field) {
    Runnable runnable = new Runnable() {
      public void run(){
        try {
          Fields f = reader.fields();
          Terms t = f.terms(field);
          
          TermsEnum te = t.iterator(null);
          
          ArrayList<BytesRef> termList = new ArrayList<BytesRef>();
          
          BytesRef termText;
          while( (termText = te.next()) != null) {
            termList.add(termText);
          }
          
          Random rand = new Random();
          
          for (int i = 0; i < numIter; ++i) {
            int idx = rand.nextInt(termList.size());
            termText = termList.get(idx);
            te = t.iterator(null);
            te.seekCeil(termText);
            DocsEnum de = te.docs(null, null);
            int doc;
            while ((doc = de.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            }
            
            de = te.docs(null, null);
            doc = -1;
            while ((doc = de.advance(doc+2)) != DocIdSetIterator.NO_MORE_DOCS) {
            }
          }
          
        }
        catch(Exception e) {
          e.printStackTrace();
        }
      }
    };
    
    Thread[] threads = new Thread[numThreads];
    for (int i = 0 ;i<numThreads;++i) {
      threads[i] = new Thread(runnable);
    }
    for (int i = 0 ;i<numThreads;++i) {
      threads[i].start();
    }
    for (int i = 0 ;i<numThreads;++i) {
      try {
        threads[i].join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
  
  static Iterable<String> buildTweetsDataSource() throws IOException{
    File inFile = new File("/Users/johnwang/bitbucket/lucene-solr/lucene/codecs/src/resources/tweets2k.txt");
    return buildDataSrc(inFile);
  }
  
  static Iterable<String> buildLongTermSrc(int numUnique, int numDocs) throws IOException {
    Random rand = new Random();
    HashSet<Long> uniques = new HashSet<Long>();
    while(uniques.size() < numUnique) {
      long v = Math.abs(rand.nextLong());
      uniques.add(v);
    }
    Long[] uniqueArr = uniques.toArray(new Long[0]);
    
    System.out.println("unique values prepared");
    
    List<String> termArr = new LinkedList<String>();
    
    DecimalFormat format = new DecimalFormat("000000000000000");
    
    BitSet bs = new BitSet();
    for (int i=0;i<numDocs;++i) {
      int n = rand.nextInt(uniqueArr.length);
      String term = format.format(uniqueArr[n].longValue());
      termArr.add(term);
      
      if (i%10000 == 0) {
        System.out.println(i+" docs prepared");
      }
    }
    
    return termArr;
  }
  
  
  public static void main(String[] args) throws Exception{
    
    Codec codec = new Lucene42Codec() {
      @Override
      public PostingsFormat getPostingsFormatForField(String field) {
        return new BoboFacetPostingsFormat();
      }
    };
    
    Iterable<String> datasrc = buildTweetsDataSource();
    
    System.out.println("dataset prepared");
    
    Directory codecIndex = buildIndex(datasrc, codec);
    

    System.out.println("new codec indexed");
    Directory luceneIndex = buildIndex(datasrc, null);
    
    System.out.println("luncene default codec indexed");
    
    //IndexComparator.showIndex(luceneIndex);
    //IndexComparator.showIndex(codecIndex);
    
    boolean same = IndexComparator.compareReaders(luceneIndex, codecIndex);
    if (!same) {
      System.out.println("comparison failed");
    }
    else {
      System.out.println("comparison success");
    }
  }
  
}
