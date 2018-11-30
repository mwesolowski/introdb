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
  private final MetaDataPage metaDataPage;
  private final DataPageSwitch dataPageSwitch;
  private final Serializer serializer;

  SingleMMapUnorderedHeapFile(final Path path, final int maxNrPages, final int pageSize) throws IOException {
    this.maxNrPages = maxNrPages;
    var mappedByteBuffer = createMappedByteBuffer(path, maxNrPages, pageSize);
    this.metaDataPage = new MetaDataPage(mappedByteBuffer, META_DATA_PAGE_INDEX, pageSize);
    this.dataPageSwitch = new DataPageSwitch(mappedByteBuffer, pageSize);
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
    final long numberOfPages = metaDataPage.getNumberOfPages();
    for (var pageNumber = 1; pageNumber <= numberOfPages; pageNumber++) {
      dataPageSwitch.loadPage(pageNumber);
      final var serializedRecordValue = dataPageSwitch.findInSelectedPage(serializedKey);
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
    dataPageSwitch.loadPage((int) metaDataPage.getNumberOfPages());
    if (!dataPageSwitch.addToSelectedPage(serializedKey, serializedValue)) {
      addNextPage();
      dataPageSwitch.addToSelectedPage(serializedKey, serializedValue);
    }
  }

  private byte[] removeRecord(final Serializable key) throws IOException {
    final var serializedKey = serializer.serialize(key);
    final long numberOfPages = metaDataPage.getNumberOfPages();
    for (var pageNumber = 1; pageNumber <= numberOfPages; pageNumber++) {
      dataPageSwitch.loadPage(pageNumber);
      final var serializedValue = dataPageSwitch.removeFromSelectedPage(serializedKey);
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
    if (metaDataPage.getNumberOfPages() == 0) {
      addNextPage();
    }
  }

  private void addNextPage() {
    final var numberOfPages = metaDataPage.getNumberOfPages();
    if (numberOfPages == maxNrPages) {
      throw new TooManyPages(maxNrPages);
    }
    final var nextPageNumber = (int) numberOfPages + 1;
    dataPageSwitch.createAndSelectPage(nextPageNumber);
    metaDataPage.setNumberOfPages(nextPageNumber);
  }

}
