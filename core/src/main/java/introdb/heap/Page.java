package introdb.heap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

class Page {
  private static final int OFFSET_FIELD_BYTES = Integer.BYTES;
  private static final int FREE_SPACE_OFFSET_OFFSET = 0;
  private static final int DATA_SEGMENT_OFFSET = FREE_SPACE_OFFSET_OFFSET + OFFSET_FIELD_BYTES;

  private final FileChannel fileChannel;
  private final int pageSize;
  private long currentPageNumber = -1;
  private final ByteBuffer pageByteBuffer;
  private final Record record = new Record();

  Page(final FileChannel fileChannel, final int pageSize) {
    this.fileChannel = fileChannel;
    this.pageSize = pageSize;
    this.pageByteBuffer = ByteBuffer.allocateDirect(pageSize);
  }

  void loadPage(final long pageNumber) throws IOException {
    if (currentPageNumber != pageNumber) {
      currentPageNumber = pageNumber;
      readPage();
    }
  }

  void createNewPage(final long pageNumber) {
    currentPageNumber = pageNumber;
    setFreeSpaceOffset(DATA_SEGMENT_OFFSET);
  }

  byte[] findInCurrentPage(final byte[] key) {
    return loadRecordWithKey(key) ? record.getValue() : null;
  }

  boolean addToCurrentPage(final byte[] key, final byte[] value) throws IOException {
    final var newRecordLength = Record.recordLengthFor(key, value);
    if (pageSize - DATA_SEGMENT_OFFSET < newRecordLength) {
      throw new IllegalArgumentException("Entry is too big.");
    }
    final var freeSpaceOffset = getFreeSpaceOffset();
    if (pageSize - freeSpaceOffset >= newRecordLength) {
      record.createNewRecord(pageByteBuffer, freeSpaceOffset, key, value);
      setFreeSpaceOffset(freeSpaceOffset + newRecordLength);
      savePage();
      return true;
    } else {
      return false;
    }
  }

  byte[] removeFromCurrentPage(final byte[] key) throws IOException {
    if (loadRecordWithKey(key)) {
      record.markAsDeleted();
      savePage();
      return record.getValue();
    } else {
      return null;
    }
  }

  private boolean loadRecordWithKey(final byte[] wantedKey) {
    final var freeSpaceOffset = getFreeSpaceOffset();
    var recordOffset = DATA_SEGMENT_OFFSET;
    while (recordOffset < freeSpaceOffset) {
      record.loadRecord(pageByteBuffer, recordOffset);
      if (record.isNotDeleted() && record.hasKey(wantedKey)) {
        return true;
      }
      recordOffset += record.getLength();
    }
    return false;
  }

  private void readPage() throws IOException {
    pageByteBuffer.clear();
    fileChannel.read(pageByteBuffer, currentPageNumber * pageSize);
  }

  private void savePage() throws IOException {
    pageByteBuffer.flip();
    fileChannel.write(pageByteBuffer, currentPageNumber * pageSize);
    pageByteBuffer.clear();
  }

  private int getFreeSpaceOffset() {
    return pageByteBuffer.getInt(FREE_SPACE_OFFSET_OFFSET);
  }

  private void setFreeSpaceOffset(final int freeSpaceOffset) {
    pageByteBuffer.putInt(FREE_SPACE_OFFSET_OFFSET, freeSpaceOffset);
  }

}
