package introdb.heap;

import java.io.IOException;
import java.io.Serializable;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

final class SingleMMapUnorderedHeapFile implements Store {
  private static final int META_DATA_PAGE_INDEX = 0;
  private final int maxNrPages;
  private final MetaDataPageOperator metaDataPageOperator;
  private final DataPageSelector dataPageSelector;
  private final Serializer serializer;

  SingleMMapUnorderedHeapFile(final Path path, final int maxNrPages, final int pageSize) throws IOException {
    this.maxNrPages = maxNrPages;
    final var mappedByteBuffer = createMappedByteBuffer(path, maxNrPages, pageSize);
    this.metaDataPageOperator = new MetaDataPageOperator(mappedByteBuffer, META_DATA_PAGE_INDEX, pageSize);
    this.dataPageSelector = new DataPageSelector(mappedByteBuffer, pageSize);
    this.serializer = new Serializer(pageSize);
    initialize();
  }

  @Override
  public Object remove(final Serializable key) throws IOException, ClassNotFoundException {
    final var serializedValue = removeRecord(key);
    return serializedValue != null ? serializer.deserialize(serializedValue) : null;
  }

  @Override
  public Object get(final Serializable key) throws IOException, ClassNotFoundException {
    final var serializedKey = serializer.serialize(key);
    final long numberOfPages = metaDataPageOperator.getNumberOfPages();
    for (var pageNumber = 1; pageNumber <= numberOfPages; pageNumber++) {
      dataPageSelector.selectPage(pageNumber);
      final var serializedRecordValue = dataPageSelector.findInSelectedPage(serializedKey);
      if (serializedRecordValue != null) {
        return serializer.deserialize(serializedRecordValue);
      }
    }
    return null;
  }

  @Override
  public void put(Entry entry) throws IOException {
    removeRecord(entry.key());
    final var serializedKey = serializer.serialize(entry.key());
    final var serializedValue = serializer.serialize(entry.value());
    dataPageSelector.selectPage((int) metaDataPageOperator.getNumberOfPages());
    if (!dataPageSelector.addToSelectedPage(serializedKey, serializedValue)) {
      addNextPage();
      dataPageSelector.addToSelectedPage(serializedKey, serializedValue);
    }
  }

  private byte[] removeRecord(final Serializable key) throws IOException {
    final var serializedKey = serializer.serialize(key);
    final long numberOfPages = metaDataPageOperator.getNumberOfPages();
    for (var pageNumber = 1; pageNumber <= numberOfPages; pageNumber++) {
      dataPageSelector.selectPage(pageNumber);
      final var serializedValue = dataPageSelector.removeFromSelectedPage(serializedKey);
      if (serializedValue != null) {
        return serializedValue;
      }
    }
    return null;
  }

  private MappedByteBuffer createMappedByteBuffer(final Path path, final int maxNrPages, final int pageSize) throws IOException {
    final var maxFileSize = maxNrPages * pageSize;
    final var fileChannel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
    return fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, maxFileSize);
  }

  private void initialize() {
    if (metaDataPageOperator.getNumberOfPages() == 0) {
      addNextPage();
    }
  }

  private void addNextPage() {
    final var numberOfPages = metaDataPageOperator.getNumberOfPages();
    final var nextPageNumber = (int) numberOfPages + 1;
    if (nextPageNumber == maxNrPages) {
      throw new TooManyPages(maxNrPages);
    }
    dataPageSelector.createAndSelectPage(nextPageNumber);
    metaDataPageOperator.setNumberOfPages(nextPageNumber);
  }

}
