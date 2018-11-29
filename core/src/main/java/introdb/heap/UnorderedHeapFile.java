package introdb.heap;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;

final class UnorderedHeapFile implements Store {
  private final Store delegate;

  UnorderedHeapFile(Path path, int maxNrPages, int pageSize) throws IOException {
    final var maximumFileSize = (long) maxNrPages * (long) pageSize;
    if (maximumFileSize <= Integer.MAX_VALUE) {
      delegate = new SingleMemoryMappedUnorderedHeapFile(path, maxNrPages, pageSize);
    } else {
      // TODO implement MultipleMemoryMappedUnorderedHeapFile
      throw new UnsupportedOperationException(
          "You've tried to create an unordered heap file of size " + maximumFileSize + " bytes. " +
              "Due to technical issues files larger than " + Integer.MAX_VALUE + " bytes are not supported yet. " +
              "But lose no hope, we have this feature on the roadmap.");
    }
  }

  @Override
  public Object remove(Serializable key) throws IOException, ClassNotFoundException {
    return delegate.remove(key);
  }

  @Override
  public Object get(Serializable key) throws IOException, ClassNotFoundException {
    return delegate.get(key);
  }

  @Override
  public void put(Entry entry) throws IOException, ClassNotFoundException {
    delegate.put(entry);
  }
}
