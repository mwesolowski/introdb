package introdb.heap;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

final class MetaDataPage {
  private static final int NUMBER_OF_PAGES_OFFSET = 0;

  private final MappedByteBuffer mappedByteBuffer;
  private final ByteBuffer pageByteBuffer;
  private final int pageOffset;

  MetaDataPage(final MappedByteBuffer mappedByteBuffer, final int pageIndex, final int pageSize) {
    this.mappedByteBuffer = mappedByteBuffer;
    this.pageOffset = pageIndex * pageSize;
    this.pageByteBuffer = createPageByteBuffer(pageSize);
  }

  private ByteBuffer createPageByteBuffer(final int pageSize) {
    var pageData = new byte[pageSize];
    mappedByteBuffer.position(pageOffset).get(pageData);
    return ByteBuffer.wrap(pageData);
  }

  long getNumberOfPages() {
    return pageByteBuffer.getLong(NUMBER_OF_PAGES_OFFSET);
  }

  void setNumberOfPages(final long numberOfPages) {
    pageByteBuffer.putLong(NUMBER_OF_PAGES_OFFSET, numberOfPages);
    mappedByteBuffer.position(this.pageOffset).put(pageByteBuffer);
  }

}
