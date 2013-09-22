package com.browseengine.bobo.api.codec;

import java.io.IOException;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.TermState;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;

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

public class BoboFacetPostingsReader extends PostingsReaderBase {

  private final IndexInput docIn;
  private final DocIdSetCollector docIdSetCollector;

  public BoboFacetPostingsReader(SegmentReadState state, DocIdSetCollector docIdSetCollector) throws IOException{    
    boolean success = false;
    this.docIdSetCollector = docIdSetCollector;
    IndexInput in = null;
    try {
      in = state.directory.openInput(IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, BoboFacetPostingsFormat.DOC_EXTENSION),
          state.context);
      CodecUtil.checkHeader(in,
          BoboFacetPostingsWriter.DOC_CODEC,
          BoboFacetPostingsWriter.VERSION_CURRENT,
          BoboFacetPostingsWriter.VERSION_CURRENT);

      this.docIn = in;
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(in);
      }
    }
  }

  @Override
  public void init(IndexInput termsIn) throws IOException {
    // Make sure we are talking to the matching postings writer
    CodecUtil.checkHeader(termsIn,
        BoboFacetPostingsWriter.TERMS_CODEC,
        BoboFacetPostingsWriter.VERSION_CURRENT,
        BoboFacetPostingsWriter.VERSION_CURRENT);
    final int indexBlockSize = termsIn.readVInt();
    if (indexBlockSize != BoboFacetPostingsWriter.BLOCK_SIZE) {
      throw new IllegalStateException("index-time BLOCK_SIZE (" + indexBlockSize + ") != read-time BLOCK_SIZE (" + BoboFacetPostingsWriter.BLOCK_SIZE + ")");
    }
  }

//Must keep final because we do non-standard clone
 private final static class IntBlockTermState extends BlockTermState {
   long docStartFP;

// Only used by the "primary" TermState -- clones don't
   // copy this (basically they are "transient"):
   ByteArrayDataInput bytesReader;  // TODO: should this NOT be in the TermState...?
   byte[] bytes;

   @Override
   public IntBlockTermState clone() {
     IntBlockTermState other = new IntBlockTermState();
     other.copyFrom(this);
     return other;
   }

   @Override
   public void copyFrom(TermState _other) {
     super.copyFrom(_other);
     IntBlockTermState other = (IntBlockTermState) _other;
     docStartFP = other.docStartFP;
   }

   @Override
   public String toString() {
     return super.toString() + " docStartFP=" + docStartFP;
   }
 }

  @Override
  public BlockTermState newTermState() throws IOException {
    return new IntBlockTermState();
  }

  @Override
  public void nextTerm(FieldInfo fieldInfo, BlockTermState _state)
      throws IOException {
    final IntBlockTermState termState = (IntBlockTermState) _state;
    final DataInput in = termState.bytesReader;
    //final boolean isFirstTerm = termState.termBlockOrd == 0;
    termState.docStartFP = in.readVLong();
  }

  @Override
  public DocsEnum docs(FieldInfo fieldInfo, BlockTermState state,
      final Bits liveDocs, DocsEnum reuse, int flags) throws IOException {
    IntBlockTermState qcstate = (IntBlockTermState)state;

    long docFP = qcstate.docStartFP;
    IndexInput in = docIn.clone();
    in.seek(docFP);
    final int df = in.readVInt();
    
    DocIdSet docSet = docIdSetCollector.load(in);
    
    final DocIdSetIterator disi = docSet.iterator();
    
    return new DocsEnum() {
      
      @Override
      public int nextDoc() throws IOException {
        if (liveDocs == null) {
          return doNextDoc();
        }
        while(true){
          int doc = doNextDoc();
          if (doc == DocIdSetIterator.NO_MORE_DOCS || liveDocs.get(doc)){
            return doc;
          }
        }
      }
      
      public int doNextDoc() throws IOException {
        return disi.nextDoc();
      }
      
      @Override
      public int docID() {
        return disi.docID();
      }
      
      @Override
      public long cost() {
        return df;
      }
      
      @Override
      public int advance(int target) throws IOException {
        int doc = doAdvance(target);
        if (liveDocs == null || doc == DocIdSetIterator.NO_MORE_DOCS) return doc;
        if (liveDocs.get(doc)) return doc;
        return nextDoc();
      }
      
      public int doAdvance(int target) throws IOException {
        return disi.advance(target);
      }
      
      @Override
      public int freq() throws IOException {
        return -1;
      }
    };
  }

  @Override
  public DocsAndPositionsEnum docsAndPositions(FieldInfo fieldInfo,
      BlockTermState state, Bits skipDocs, DocsAndPositionsEnum reuse, int flags)
      throws IOException {
    return null;
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(docIn);
  }

  @Override
  public void readTermsBlock(IndexInput termsIn, FieldInfo fieldInfo,
      BlockTermState _termState) throws IOException {
    final IntBlockTermState termState = (IntBlockTermState) _termState;
    final int numBytes = termsIn.readVInt();

    if (termState.bytes == null) {
      termState.bytes = new byte[ArrayUtil.oversize(numBytes, 1)];
      termState.bytesReader = new ByteArrayDataInput();
    } else if (termState.bytes.length < numBytes) {
      termState.bytes = new byte[ArrayUtil.oversize(numBytes, 1)];
    }

    termsIn.readBytes(termState.bytes, 0, numBytes);
    termState.bytesReader.reset(termState.bytes, 0, numBytes);
  }

}
