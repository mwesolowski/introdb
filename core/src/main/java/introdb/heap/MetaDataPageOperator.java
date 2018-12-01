package introdb.heap;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

final class MetaDataPageOperator {
  private static final int NUMBER_OF_PAGES_OFFSET = 0;

  private final MappedByteBuffer mappedByteBuffer;
  private final byte[] pageData;
  private final ByteBuffer pageDataByteBuffer;
  private final int pageOffset;

  MetaDataPageOperator(final MappedByteBuffer mappedByteBuffer, final int pageIndex, final int pageSize) {
    this.mappedByteBuffer = mappedByteBuffer;
    this.pageOffset = pageIndex * pageSize;
    this.pageData = new byte[pageSize];
    this.pageDataByteBuffer = ByteBuffer.wrap(pageData);
    loadPageDataFromMemoryMappedBuffer();
  }

  long getNumberOfPages() {
    return pageDataByteBuffer.getLong(NUMBER_OF_PAGES_OFFSET);
  }

  void setNumberOfPages(final long numberOfPages) {
    pageDataByteBuffer.putLong(NUMBER_OF_PAGES_OFFSET, numberOfPages);
    savePageDataToMemoryMappedBuffer();
  }

  private void loadPageDataFromMemoryMappedBuffer() {
    mappedByteBuffer.position(pageOffset).get(pageData);
  }

  private void savePageDataToMemoryMappedBuffer() {
    mappedByteBuffer.position(this.pageOffset).put(pageData);
  }

}
