package introdb.heap;

final class MetaDataPage extends Page {
  private static final int NUMBER_OF_PAGES_OFFSET = PAGE_CONTENT_OFFSET;

  MetaDataPage(final byte[] bytes) {
    super(bytes);
  }

  long getNumberOfPages() {
    return byteBuffer.getLong(NUMBER_OF_PAGES_OFFSET);
  }

  void setNumberOfPages(final long numberOfPages) {
    byteBuffer.putLong(NUMBER_OF_PAGES_OFFSET, numberOfPages);
  }
}
