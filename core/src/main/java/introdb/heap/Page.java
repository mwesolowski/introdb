package introdb.heap;

import introdb.heap.serialization.Serializer;

import java.nio.ByteBuffer;

abstract class Page {
  private static final int PAGE_NUMBER_OFFSET = 0;
  static final int OFFSET_FIELD_BYTES = Integer.BYTES;
  static final int PAGE_CONTENT_OFFSET = PAGE_NUMBER_OFFSET + OFFSET_FIELD_BYTES;

  final byte[] bytes;
  final ByteBuffer byteBuffer;
  final Serializer serializer;

  Page(final byte[] bytes, final Serializer serializer) {
    this.bytes = bytes;
    this.byteBuffer = ByteBuffer.wrap(bytes);
    this.serializer = serializer;
  }

  int getPageNumber() {
    return byteBuffer.getInt(PAGE_NUMBER_OFFSET);
  }

  void setPageNumber(final int pageNumber) {
    byteBuffer.putInt(PAGE_NUMBER_OFFSET, pageNumber);
  }

}
