package introdb.heap;

import introdb.heap.serialization.JdkSerializer;
import introdb.heap.serialization.Serializer;

import java.io.IOException;
import java.io.Serializable;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class SingleMemoryMappedUnorderedHeapFile implements Store {
  private static final int META_DATA_PAGE_OFFSET = 0;

  private final int pageSize;
  private final int maxNrPages;
  private final Serializer serializer;
  private final MappedByteBuffer mappedByteBuffer;
  private final MetaDataPage metaDataPage;

  SingleMemoryMappedUnorderedHeapFile(Path path, int maxNrPages, int pageSize) throws IOException {
    this.pageSize = pageSize;
    this.maxNrPages = maxNrPages;
    this.serializer = new JdkSerializer(pageSize);
    this.mappedByteBuffer = createMappedByteBuffer(path, maxNrPages, pageSize);
    this.metaDataPage = createMetaDataPage(pageSize, mappedByteBuffer);
    initializeMetaData();
  }

  @Override
  public Object remove(Serializable key) throws IOException, ClassNotFoundException {
    for (var pageNumber = 1; pageNumber <= metaDataPage.getNumberOfPages(); pageNumber++) {
      var page = readPage(pageNumber);
      var record = page.removeRecord(key);
      if (record != null) {
        writePage(page);
        return record.getValue();
      }
    }
    return null;
  }

  @Override
  public Object get(Serializable key) throws IOException, ClassNotFoundException {
    for (var pageNumber = 1; pageNumber <= metaDataPage.getNumberOfPages(); pageNumber++) {
      var record = (readPage(pageNumber)).getRecord(key);
      if (record != null) {
        return record.getValue();
      }
    }
    return null;
  }

  @Override
  public void put(Entry entry) throws IOException, ClassNotFoundException {
    remove(entry.key());

    var record = new Record(entry.key(), entry.value());
    var lastPage = lastPage();
    if (lastPage.addRecord(record)) {
      writePage(lastPage);
    } else {
      var newPage = addNextPage();
      newPage.addRecord(record);
      writePage(newPage);
    }
  }

  private MappedByteBuffer createMappedByteBuffer(Path path, int maxNrPages, int pageSize) throws IOException {
    final var maxFileSize = maxNrPages * pageSize;
    final var fileChannel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
    return fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, maxFileSize);
  }

  private MetaDataPage createMetaDataPage(final int pageSize, final MappedByteBuffer mappedByteBuffer) {
    var buff = new byte[pageSize];
    mappedByteBuffer.position(META_DATA_PAGE_OFFSET).get(buff);
    return new MetaDataPage(buff, serializer);
  }

  private void initializeMetaData() {
    if (metaDataPage.getNumberOfPages() == 0) {
      addNextPage();
    }
  }

  private DataPage readPage(int pageNumber) {
    var pageOffset = pageSize * pageNumber;
    var bytes = new byte[pageSize];
    mappedByteBuffer.position(pageOffset).get(bytes);
    return DataPage.existingPage(bytes, serializer);
  }

  private void writePage(final Page page) {
    var pageOffset = pageSize * page.getPageNumber();
    mappedByteBuffer.position(pageOffset).put(page.bytes);
  }

  private DataPage addNextPage() {
    var numberOfPages = metaDataPage.getNumberOfPages();
    if (numberOfPages == maxNrPages) {
      throw new TooManyPages(maxNrPages);
    }
    var pageNumber = (int) numberOfPages + 1;
    return createPage(pageNumber);
  }

  private DataPage createPage(final int pageNumber) {
    var page = DataPage.newPage(pageNumber, new byte[pageSize], serializer);
    metaDataPage.setNumberOfPages(pageNumber);
    writePage(page);
    writePage(metaDataPage);
    return page;
  }

  private DataPage lastPage() {
    var numberOfPages = metaDataPage.getNumberOfPages();
    return readPage((int) numberOfPages);
  }

}
