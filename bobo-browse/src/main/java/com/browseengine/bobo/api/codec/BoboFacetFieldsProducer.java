package com.browseengine.bobo.api.codec;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

public class BoboFacetFieldsProducer extends FieldsProducer {  
  private IndexInput in;
  private final PostingsReaderBase postingsReader;
  private Map<String, TermMetaWithState[]> fieldTermMap;
  private Map<String, BytesRef[]> termsMap;
  private Map<String, FieldMeta> fieldMetaMap;
  
  static class FieldMeta {
    FieldInfo field;
    int docCount;
    int termCount;
    long sumTotalTermFreq;
    long sumDocFreq;
  }
  
  static class TermMetaWithState {
    int df;
    long totalTf;
    BlockTermState termState;
  }
  
  BoboFacetFieldsProducer(SegmentReadState state, PostingsReaderBase postingsReader) throws IOException{
    this.postingsReader = postingsReader;
    
    fieldTermMap = new HashMap<String, TermMetaWithState[]>();
    fieldMetaMap = new HashMap<String, FieldMeta>();
    termsMap = new HashMap<String, BytesRef[]>();
    
    in = state.directory.openInput(IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, BoboFacetFieldsConsumer.EXT),
        state.context);
    int version = CodecUtil.checkHeader(in, BoboFacetFieldsConsumer.CODEC,0,0);
    if (version != BoboFacetFieldsConsumer.VERSION) {
      throw new IOException("invalid version: " + version);
    }

    int fieldCount = in.readVInt();
    postingsReader.init(in);
    for (int i = 0; i < fieldCount; ++i) {
      FieldMeta fieldMeta = new FieldMeta();
      
      int fieldId = in.readVInt();
      
      fieldMeta.field = state.fieldInfos.fieldInfo(fieldId);
      
      fieldMetaMap.put(fieldMeta.field.name, fieldMeta);
          
      fieldMeta.termCount = in.readVInt();
      
      BytesRef[] terms = new BytesRef[fieldMeta.termCount];
      TermMetaWithState[] metaList = new TermMetaWithState[terms.length];
      for (int k = 0; k <terms.length; ++k) {
        terms[k] = new BytesRef(in.readString());
        metaList[k] = new TermMetaWithState();
        metaList[k].df = in.readVInt();
        metaList[k].totalTf = in.readLong();
      }
      
      fieldMeta.sumTotalTermFreq = in.readLong();
      fieldMeta.sumDocFreq = in.readLong();
      fieldMeta.docCount = in.readVInt();
      
      
      BlockTermState termState = postingsReader.newTermState();
      
      postingsReader.readTermsBlock(in, fieldMeta.field, termState);
      
      
      for (int k = 0; k <terms.length; ++k) {
        TermMetaWithState metaState = metaList[k];
        termState.docFreq = metaList[k].df;
        termState.termBlockOrd = k+1;
        termState.totalTermFreq = metaList[k].totalTf;
        postingsReader.nextTerm(fieldMeta.field, termState);
        metaState.termState = postingsReader.newTermState();
        metaState.termState.copyFrom(termState);
      }
      
      fieldTermMap.put(fieldMeta.field.name, metaList);
      termsMap.put(fieldMeta.field.name, terms);
    }
  }
  
  @Override
  public void close() throws IOException {
    IOUtils.closeWhileHandlingException(in, postingsReader);
  }

  @Override
  public Iterator<String> iterator() {
    return fieldMetaMap.keySet().iterator();
  }

  @Override
  public Terms terms(String field) throws IOException {
    
    TermMetaWithState[] termMetas = fieldTermMap.get(field);
    
    if (termMetas == null) {
      return null;
    }
    
    FieldMeta fmeta = fieldMetaMap.get(field);
    
    BytesRef[] terms = termsMap.get(field);
    
    return new BoboFacetHashTerms(terms, termMetas, fmeta, postingsReader,BytesRef.getUTF8SortedAsUnicodeComparator());
  }

  @Override
  public int size() {
    return fieldMetaMap.size();
  }

}
