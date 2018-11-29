package introdb.heap;

import java.io.IOException;
import java.io.Serializable;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

final class SingleMemoryMappedUnorderedHeapFile implements Store {
  private static final int META_DATA_PAGE_INDEX = 0;
  private final int pageSize;
  private final int maxNrPages;
  private final Serializer serializer;
  private final MappedByteBuffer mappedByteBuffer;
  private final MetaDataPage metaDataPage;

  SingleMemoryMappedUnorderedHeapFile(final Path path, final int maxNrPages, final int pageSize) throws IOException {
    this.pageSize = pageSize;
    this.maxNrPages = maxNrPages;
    this.serializer = new Serializer(pageSize);
    this.mappedByteBuffer = createMappedByteBuffer(path, maxNrPages, pageSize);
    this.metaDataPage = createMetaDataPage(pageSize, mappedByteBuffer);
    initializeMetaData();
  }

  @Override
  public Object remove(final Serializable key) throws IOException, ClassNotFoundException {
    final var serializedValue = removeRecord(key);
    return serializedValue != null ? serializer.deserialize(serializedValue) : null;
  }

  @Override
  public Object get(final Serializable key) throws IOException, ClassNotFoundException {
    final var serializedKey = serializer.serialize(key);
    for (var pageNumber = 1; pageNumber <= metaDataPage.getNumberOfPages(); pageNumber++) {
      final var serializedRecordValue = (readPage(pageNumber)).getRecordValue(serializedKey);
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
    var lastPage = lastPage();
    if (lastPage.addRecord(serializedKey, serializedValue)) {
      writePage(lastPage);
    } else {
      final var newPage = addNextPage();
      newPage.addRecord(serializedKey, serializedValue);
      writePage(newPage);
    }
  }

  private byte[] removeRecord(final Serializable key) throws IOException {
    final var serializedKey = serializer.serialize(key);
    for (var pageNumber = 1; pageNumber <= metaDataPage.getNumberOfPages(); pageNumber++) {
      final var page = readPage(pageNumber);
      final var serializedValue = page.removeRecord(serializedKey);
      if (serializedValue != null) {
        writePage(page);
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

  private MetaDataPage createMetaDataPage(final int pageSize, final MappedByteBuffer mappedByteBuffer) {
    final var buff = new byte[pageSize];
    mappedByteBuffer.position(META_DATA_PAGE_INDEX * pageSize).get(buff);
    return new MetaDataPage(buff);
  }

  private void initializeMetaData() {
    if (metaDataPage.getNumberOfPages() == 0) {
      addNextPage();
    }
  }

  private DataPage readPage(final int pageNumber) {
    final var pageOffset = pageSize * pageNumber;
    final var bytes = new byte[pageSize];
    mappedByteBuffer.position(pageOffset).get(bytes);
    return DataPage.existingPage(bytes);
  }

  private void writePage(final Page page) {
    final var pageOffset = pageSize * page.getPageNumber();
    mappedByteBuffer.position(pageOffset).put(page.bytes);
  }

  private DataPage addNextPage() {
    final var numberOfPages = metaDataPage.getNumberOfPages();
    if (numberOfPages == maxNrPages) {
      throw new TooManyPages(maxNrPages);
    }
    final var pageNumber = (int) numberOfPages + 1;
    return createPage(pageNumber);
  }

  private DataPage createPage(final int pageNumber) {
    final var page = DataPage.newPage(pageNumber, new byte[pageSize]);
    metaDataPage.setNumberOfPages(pageNumber);
    writePage(page);
    writePage(metaDataPage);
    return page;
  }

  private DataPage lastPage() {
    final var numberOfPages = metaDataPage.getNumberOfPages();
    return readPage((int) numberOfPages);
  }

}
