package com.browseengine.bobo.api.codec;

import java.io.IOException;

import org.apache.lucene.codecs.BlockTreeTermsReader;
import org.apache.lucene.codecs.BlockTreeTermsWriter;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.IOUtils;

public class BoboFacetPostingsFormat extends PostingsFormat {

  private static final String NAME = "BoboPostingsFormat";
  private final DocIdSetCollector docIdSetCollector;
  public BoboFacetPostingsFormat() {
    super(NAME);
    docIdSetCollector = getPostingsDocCollector();
  }
  
  public static final String DOC_EXTENSION = "doc";
  
  protected DocIdSetCollector getPostingsDocCollector() {
    return new OpenBitsetDocIdSetCollector();
  }
  
  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state)
      throws IOException {
    PostingsWriterBase docsWriter = null;

    boolean success = false;
    try {
      docsWriter = new BoboFacetPostingsWriter(state, docIdSetCollector);
      FieldsConsumer ret = new BlockTreeTermsWriter(state, docsWriter, BlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE, BlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE);
      success = true;
      return ret;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(docsWriter);
      }
    }
  }
  
  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state)
      throws IOException {
    PostingsReaderBase docsReader = null;

    boolean success = false;
    try {
      docsReader = new BoboFacetPostingsReader(state, docIdSetCollector);
      FieldsProducer ret = new BlockTreeTermsReader(
                                                    state.directory, state.fieldInfos, state.segmentInfo,
                                                    docsReader,
                                                    state.context,
                                                    state.segmentSuffix,
                                                    state.termsIndexDivisor);
      success = true;
      return ret;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(docsReader);
      }
    }
  }
  
}
