package org.apache.lucene.index;

import java.io.IOException;
import java.util.List;

import com.browseengine.bobo.api.BoboIndexReader;

public class BoboDirectoryReader extends DirectoryReader {

  private final DirectoryReader in;
  protected BoboDirectoryReader(DirectoryReader in) {
    super(in.directory(), wrap(in.leaves()));
    this.in = in;
  }
  
  private static BoboIndexReader[] wrap(List<AtomicReaderContext> readerContexts) {
    BoboIndexReader[] wrapped = new BoboIndexReader[readerContexts.size()];
    int i = 0;
    for (AtomicReaderContext ctx : readerContexts){
      wrapped[i++] = new BoboIndexReader(ctx.reader());
    }
    return wrapped;
  }

  @Override
  protected DirectoryReader doOpenIfChanged() throws IOException {
    DirectoryReader d = in.doOpenIfChanged();
    return d == null ? null : new BoboDirectoryReader(d);
  }

  @Override
  protected BoboDirectoryReader doOpenIfChanged(IndexCommit commit)
      throws IOException {
    DirectoryReader d = in.doOpenIfChanged(commit);
    return d == null ? null : new BoboDirectoryReader(d);
  }

  @Override
  protected DirectoryReader doOpenIfChanged(IndexWriter writer,
      boolean applyAllDeletes) throws IOException {

    DirectoryReader d = in.doOpenIfChanged(writer,applyAllDeletes);
    return d == null ? null : new BoboDirectoryReader(d);
  }

  @Override
  public long getVersion() {
    return in.getVersion();
  }

  @Override
  public boolean isCurrent() throws IOException {
    return in.isCurrent();
  }

  @Override
  public IndexCommit getIndexCommit() throws IOException {
    return in.getIndexCommit();
  }

  @Override
  protected void doClose() throws IOException {
    in.doClose();
  }
}
