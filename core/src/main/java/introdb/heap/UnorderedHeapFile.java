package introdb.heap;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.WeakHashMap;

final class UnorderedHeapFile implements Store {
  private final int maxNrPages;
  private final Page page;
  private final Serializer serializer;
  private final Map<Object, Long> recordPageNumberCache = new WeakHashMap<>();
  private long numberOfPages;

  UnorderedHeapFile(final Path path, final int maxNrPages, final int pageSize) throws IOException {
    final var fileChannel =
        FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
    this.maxNrPages = maxNrPages;
    this.numberOfPages = fileChannel.size() / pageSize;
    this.page = new Page(fileChannel, pageSize);
    this.serializer = new Serializer(pageSize);
    addFirstPageIfNoneExistsYet();
  }

  private void addFirstPageIfNoneExistsYet() {
    if (numberOfPages == 0) {
      addNextPage();
    }
  }

  @Override
  public synchronized Object get(final Serializable key) throws IOException, ClassNotFoundException {
    final var serializedKey = serializer.serialize(key);
    final var startFromPage = recordPageNumberCache.getOrDefault(key, 0L);
    for (var pageNumber = startFromPage; pageNumber < numberOfPages; pageNumber++) {
      page.loadPage(pageNumber);
      final var serializedValue = page.findInCurrentPage(serializedKey);
      if (serializedValue != null) {
        recordPageNumberCache.put(key, pageNumber);
        return serializer.deserialize(serializedValue);
      }
    }
    return null;
  }

  @Override
  public synchronized void put(final Entry entry) throws IOException {
    removeRecord(entry.key());
    final var serializedKey = serializer.serialize(entry.key());
    final var serializedValue = serializer.serialize(entry.value());
    page.loadPage(numberOfPages - 1);
    if (!page.addToCurrentPage(serializedKey, serializedValue)) {
      addNextPage();
      page.addToCurrentPage(serializedKey, serializedValue);
    }
    recordPageNumberCache.put(entry.key(), numberOfPages - 1);
  }

  @Override
  public synchronized Object remove(final Serializable key) throws IOException, ClassNotFoundException {
    final var serializedValue = removeRecord(key);
    return serializedValue != null ? serializer.deserialize(serializedValue) : null;
  }

  private byte[] removeRecord(final Serializable key) throws IOException {
    final var serializedKey = serializer.serialize(key);
    final var startFromPage = recordPageNumberCache.getOrDefault(key, 0L);
    for (var pageNumber = startFromPage; pageNumber < numberOfPages; pageNumber++) {
      page.loadPage(pageNumber);
      final var serializedValue = page.removeFromCurrentPage(serializedKey);
      if (serializedValue != null) {
        return serializedValue;
      }
    }
    return null;
  }

  private void addNextPage() {
    if (numberOfPages == maxNrPages) {
      throw new TooManyPages(maxNrPages);
    }
    page.createNewPage(numberOfPages);
    numberOfPages++;
  }
}
