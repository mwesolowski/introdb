package introdb.heap;

import introdb.heap.serialization.Serializer;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

final class DataPage extends Page {
  private static final int FREE_SPACE_OFFSET_OFFSET = PAGE_CONTENT_OFFSET;
  private static final int DATA_SEGMENT_OFFSET = FREE_SPACE_OFFSET_OFFSET + OFFSET_FIELD_BYTES;
  private static final int RECORD_SIZE_FIELD_BYTES = Integer.BYTES;

  private DataPage(final byte[] bytes, final Serializer serializer) {
    super(bytes, serializer);
  }

  static DataPage newPage(final int pageNr, final byte[] bytes, final Serializer serializer) {
    assert(pageNr > 0); // Data page number must be positive. Page 0 is reserved for metadata.
    var page = new DataPage(bytes, serializer);
    page.setPageNumber(pageNr);
    page.setFreeSpaceOffset(DATA_SEGMENT_OFFSET);
    return page;
  }

  static DataPage existingPage(final byte[] bytes, final Serializer serializer) {
    var page = new DataPage(bytes, serializer);
    assert(page.getPageNumber() > 0); // Data page number must be positive. Page 0 is reserved for metadata.
    return page;
  }

  boolean addRecord(final Record record) throws IOException {
    if (canRecordBeAddedToPage(record)) {
      final var newFreeSpaceOffset = writeRecord(record, getFreeSpaceOffset());
      setFreeSpaceOffset(newFreeSpaceOffset);
      return true;
    } else {
      return false;
    }
  }

  private boolean canRecordBeAddedToPage(final Record record) throws IOException {
    final var recordDataLength = serializer.serialize(record).length + RECORD_SIZE_FIELD_BYTES;
    if (DATA_SEGMENT_OFFSET + recordDataLength > byteBuffer.capacity()) {
      throw new IllegalArgumentException("Entry is too big.");
    }
    return recordDataLength <= byteBuffer.capacity() - getFreeSpaceOffset();
  }

  Record removeRecord(final Serializable key) throws IOException, ClassNotFoundException {
    var offset = findRecordOffset(key);
    if (offset > -1) {
      var record = readRecord(offset);
      record.markAsDeleted();
      writeRecord(record, offset);
      return record;
    } else {
      return null;
    }
  }

  Record getRecord(final Serializable key) throws IOException, ClassNotFoundException {
    var offset = findRecordOffset(key);
    return offset > -1 ? readRecord(offset) : null;
  }

  private int getFreeSpaceOffset() {
    return byteBuffer.getInt(FREE_SPACE_OFFSET_OFFSET);
  }

  private void setFreeSpaceOffset(final int freeSpaceOffset) {
    byteBuffer.putInt(FREE_SPACE_OFFSET_OFFSET, freeSpaceOffset);
  }

  private int findRecordOffset(final Serializable key) throws IOException, ClassNotFoundException {
    var offset = DATA_SEGMENT_OFFSET;
    var length = byteBuffer.getInt(offset);
    while (offset < getFreeSpaceOffset()) {
      var record = readRecord(offset);
      if (Objects.equals(record.getKey(), key) && !record.isDeleted()) {
        return offset;
      } else {
        offset += RECORD_SIZE_FIELD_BYTES + length;
        length = byteBuffer.getInt(offset);
      }
    }
    return -1;
  }

  private Record readRecord(final int offset) throws IOException, ClassNotFoundException {
    var length = byteBuffer.getInt(offset);
    final var serializedRecord = new byte[length];
    byteBuffer.position(offset + RECORD_SIZE_FIELD_BYTES).get(serializedRecord);
    return (Record) serializer.deserialize(serializedRecord);
  }

  private int writeRecord(final Record record, final int offset) throws IOException {
    final var serializedRecord = serializer.serialize(record);
    final var serializedRecordLength = serializedRecord.length;
    byteBuffer.position(offset).putInt(serializedRecordLength).put(serializedRecord);
    return offset + RECORD_SIZE_FIELD_BYTES + serializedRecordLength;
  }

}
