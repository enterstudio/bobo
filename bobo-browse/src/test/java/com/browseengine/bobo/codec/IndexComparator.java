package com.browseengine.bobo.codec;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

public class IndexComparator {

  static boolean compareReaders(Directory d1, Directory d2) throws IOException{
    DirectoryReader r1 = DirectoryReader.open(d1);
    DirectoryReader r2 = DirectoryReader.open(d2);
    
    try {
      AtomicReader ar1 = r1.leaves().get(0).reader();
      AtomicReader ar2 = r2.leaves().get(0).reader();
      return compareReaders(ar1, ar2);
    }
    finally {
      IOUtils.close(r1, r2);
    }
    
  }
  
  static boolean compareReaders(AtomicReader r1, AtomicReader r2) throws IOException{
    Fields f1 = r1.fields();
    Fields f2 = r2.fields();
    
    boolean success = true;
    Iterator<String> fnames = f1.iterator();
    
    while(fnames.hasNext()) {
      String fname = fnames.next();
      
      System.out.println("field: "+fname);
      Terms t1 = f1.terms(fname);
      Terms t2 = f2.terms(fname);
      TermsEnum te1 = t1.iterator(null);
      TermsEnum te2 = t2.iterator(null);
      
      Comparator<BytesRef> comp1 = t1.getComparator();
      Comparator<BytesRef> comp2 = t2.getComparator();
      
      if (!comp1.equals(comp2)) {
        System.out.println("different term comparators");
        return false;
      }
      
      BytesRef term;
      while ((term = te1.next()) != null) {
        BytesRef term2 = te2.next();
        
        if (comp1.compare(term, term2) != 0) {
          System.out.println("term comparison failed: "+term.utf8ToString()+" != " +term2.utf8ToString());
          return false;
        }
        
        DocsEnum d1 = te1.docs(null, null);
        DocsEnum d2 = te2.docs(null, null);
        
        int doc;
        while ((doc = d1.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          int doc2 = d2.nextDoc();
          if (doc != doc2) {
            System.out.println("comparison failed for term: " + term.utf8ToString()+", docs: "+doc+" != " + doc2);
            return false;
          }
          // TODO: test freq & positions
          int freq1 = d1.freq();
          int freq2 = d2.freq();
        }
      }
    }
    
    return success;
  }
  
static void testWalkingPostings(AtomicReader reader, String field, boolean doSkipping) throws Exception{
    
    Terms terms = reader.fields().terms(field);
    TermsEnum iter = terms.iterator(null);
    BytesRef term;
    while ((term = iter.next()) != null) {
      DocsEnum de = iter.docs(null, null);
      int docid = -1;
      if (doSkipping) {
        while ((docid = de.advance(docid+2)) != DocIdSetIterator.NO_MORE_DOCS) {
        }
      }
      else {
        while ((docid = de.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        }
      }
    }
  }
  
  static AtomicReader extractReader(Directory dir) throws Exception{
    DirectoryReader reader = DirectoryReader.open(dir);
    AtomicReader ar = reader.leaves().get(0).reader();
 
    return ar;
  }

  static void showIndex(Directory dir) throws Exception{
    DirectoryReader reader = DirectoryReader.open(dir);
    AtomicReader ar = reader.leaves().get(0).reader();
    
    Fields fields = ar.fields();
    for (String field : fields) {
      Terms terms = fields.terms(field);
      TermsEnum iter = terms.iterator(null);
      BytesRef term;
      while ((term = iter.next()) != null) {
        System.out.println(term.utf8ToString());
        DocsEnum de = iter.docs(null, null);
        int docid;
        while ((docid = de.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          System.out.println(docid);
        }
      }
    }

    reader.close();
  }
}
