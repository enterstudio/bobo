package com.browseengine.bobo.api.codec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.PostingsConsumer;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.codecs.TermsConsumer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

public class BoboFacetFieldsConsumer extends FieldsConsumer {

  private TreeMap<String, PostingsConsumer> termHash = new TreeMap<String, PostingsConsumer>();
  
  private final SegmentWriteState state;
  private final PostingsWriterBase postingsWriter;
  
  static final String EXT = "bto";
  static final String CODEC = "BoboPrimitiveHashCodec";
  static final int VERSION = 0;
  
  
  private IndexOutput out;
  
  public BoboFacetFieldsConsumer(SegmentWriteState state, PostingsWriterBase postingsWriter) throws IOException{
    this.state = state;
    this.postingsWriter = postingsWriter;
    
    final String termsFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, EXT);
    out = state.directory.createOutput(termsFileName, state.context);
    
    CodecUtil.writeHeader(out, CODEC, VERSION);
    out.writeVInt(state.fieldInfos.size());
    postingsWriter.start(out);
  }

  @Override
  public TermsConsumer addField(FieldInfo field) throws IOException {
    return new TermsWriter(field);
  }

  @Override
  public void close() throws IOException {
    IOUtils.closeWhileHandlingException(out, postingsWriter);
  }
  
  private class TermsWriter extends TermsConsumer {
    private final int fieldId;
    List<BytesRef> termList = new ArrayList<BytesRef>();
    List<TermStats> termStats = new ArrayList<TermStats>();
    
    TermsWriter(FieldInfo field) {
      fieldId = field.number;
      postingsWriter.setField(field);
    }
    @Override
    public PostingsConsumer startTerm(BytesRef text) throws IOException {
      
      postingsWriter.startTerm();
      return postingsWriter;
    }

    @Override
    public void finishTerm(BytesRef text, TermStats stats) throws IOException {
      // write stats info out to somewhere, this is per term
      termList.add(text.clone());
      termStats.add(stats);
      postingsWriter.finishTerm(stats);
    }

    @Override
    public void finish(long sumTotalTermFreq, long sumDocFreq, int docCount)
        throws IOException {
      
      out.writeVInt(fieldId);
      BytesRef[] termArr = termList.toArray(new BytesRef[termList.size()]);
      
      out.writeVInt(termArr.length);
      for (int i=0; i<termArr.length; ++i) {
        BytesRef term = termArr[i];
        TermStats stats = termStats.get(i);
        out.writeString(term.utf8ToString());
        out.writeVInt(stats.docFreq);
        out.writeLong(stats.totalTermFreq);
      }
      out.writeLong(sumTotalTermFreq);
      out.writeLong(sumDocFreq);
      out.writeVInt(docCount);
      postingsWriter.flushTermsBlock(termArr.length, termArr.length); // all of it
      
    }

    @Override
    public Comparator<BytesRef> getComparator() throws IOException {
      return BytesRef.getUTF8SortedAsUnicodeComparator();
    }
    
  }

}
