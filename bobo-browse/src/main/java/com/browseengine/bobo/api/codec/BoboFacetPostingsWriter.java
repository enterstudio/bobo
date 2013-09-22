package com.browseengine.bobo.api.codec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.BytesRef;
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

public class BoboFacetPostingsWriter extends PostingsWriterBase {

  final static String DOC_CODEC = "BoboFacetPostingsWriterDoc";
  final static String TERMS_CODEC = "BoboFacetPostingsWriterTerms";

  public final static int BLOCK_SIZE = 128;

//Increment version to change it
 final static int VERSION_START = 0;
 final static int VERSION_CURRENT = VERSION_START;
  final IndexOutput docOut;
  private IndexOutput termsOut;

  private long docTermStartFP;
  private int currentDoc = -1;
  
  private final int maxDoc;
  private final DocIdSetCollector docidCollector;
  
  public BoboFacetPostingsWriter(SegmentWriteState state, DocIdSetCollector docidCollector) throws IOException{
    this.docidCollector = docidCollector;
    maxDoc = state.segmentInfo.getDocCount();

    docOut = state.directory.createOutput(IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix,
        BoboFacetPostingsFormat.DOC_EXTENSION),
        state.context);
    
    boolean success = false;

    try {
      CodecUtil.writeHeader(docOut, DOC_CODEC, VERSION_CURRENT);
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(docOut);
      }
    }
  }

  @Override
  public void start(IndexOutput termsOut) throws IOException {
    this.termsOut = termsOut;
    CodecUtil.writeHeader(termsOut, TERMS_CODEC, VERSION_CURRENT);
    termsOut.writeVInt(BLOCK_SIZE);
  }

  @Override
  public void startTerm() throws IOException {
    docTermStartFP = docOut.getFilePointer();
    docidCollector.reset(maxDoc);
  }

  private final RAMOutputStream bytesWriter = new RAMOutputStream();

  @Override
  public void flushTermsBlock(int start, int count) throws IOException {
    if (count == 0) {
      termsOut.writeByte((byte) 0);
      return;
    }

    assert start <= pendingTerms.size();
    assert count <= start;

    final int limit = pendingTerms.size() - start + count;

    for(int idx=limit-count; idx<limit; idx++) {
      PendingTerm term = pendingTerms.get(idx);
      bytesWriter.writeVLong(term.docStartFP);
    }

    termsOut.writeVInt((int) bytesWriter.getFilePointer());
    bytesWriter.writeTo(termsOut);
    bytesWriter.reset();
    termsOut.flush();

    // Remove the terms we just wrote:
    pendingTerms.subList(limit-count, limit).clear();
  }

  private static class PendingTerm {
    public final long docStartFP;

    public PendingTerm(long docStartFP) {
      this.docStartFP = docStartFP;
    }
  }

  private final List<PendingTerm> pendingTerms = new ArrayList<PendingTerm>();

  @Override
  public void finishTerm(TermStats stats) throws IOException {
    pendingTerms.add(new PendingTerm(docTermStartFP));
    
    // write it out!

    docOut.writeVInt(stats.docFreq);    
    docidCollector.flush(stats, docOut);
    docOut.flush();
  }

  @Override
  public void setField(FieldInfo fieldInfo) {
    assert IndexOptions.DOCS_ONLY == fieldInfo.getIndexOptions();
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(docOut);
  }

  @Override
  public void startDoc(int docID, int freq) throws IOException {
    currentDoc = docID;
  }

  @Override
  public void addPosition(int position, BytesRef payload, int startOffset,
      int endOffset) throws IOException {
    // we don't handle positions for facet fields
  }

  @Override
  public void finishDoc() throws IOException {    
    docidCollector.collect(currentDoc);
  }
}
