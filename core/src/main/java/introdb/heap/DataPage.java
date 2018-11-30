package introdb.heap;

import java.nio.ByteBuffer;
import java.util.Arrays;

final class DataPage {
  private static final int DELETED_FLAG_BYTES = 1;
  private static final int SIZE_FIELD_BYTES = Integer.BYTES;
  private static final int RECORD_META_DATA_BYTES = DELETED_FLAG_BYTES + 2 * SIZE_FIELD_BYTES;
  private static final int OFFSET_FIELD_BYTES = Integer.BYTES;

  private static final int FREE_SPACE_OFFSET_OFFSET = 0;
  private static final int DATA_SEGMENT_OFFSET = FREE_SPACE_OFFSET_OFFSET + OFFSET_FIELD_BYTES;

  private final ByteBuffer byteBuffer;

  private DataPage(final byte[] bytes) {
    this.byteBuffer = ByteBuffer.wrap(bytes);
  }

  static DataPage newPage(final byte[] bytes) {
    final var page = new DataPage(bytes);
    page.setFreeSpaceOffset(DATA_SEGMENT_OFFSET);
    return page;
  }

  static DataPage existingPage(final byte[] bytes) {
    return new DataPage(bytes);
  }

  boolean addRecord(final byte[] key, final byte[] value) {
    final var recordDataLength = RECORD_META_DATA_BYTES + key.length + value.length;
    final var bufferCapacity = byteBuffer.capacity();
    final var freeSpaceOffset = getFreeSpaceOffset();
    final var newFreeSpaceOffset = freeSpaceOffset + recordDataLength;
    if (DATA_SEGMENT_OFFSET + recordDataLength > bufferCapacity) {
      throw new IllegalArgumentException("Entry is too big.");
    }
    if (newFreeSpaceOffset <= bufferCapacity) {
      writeRecord(freeSpaceOffset, key, value);
      setFreeSpaceOffset(newFreeSpaceOffset);
      return true;
    } else {
      return false;
    }
  }

  byte[] removeRecord(final byte[] key) {
    final var recordOffset = findRecordOffset(key);
    if (recordOffset > -1) {
      byteBuffer.position(recordOffset).put((byte) 1);
      return readRecordValue(recordOffset);
    } else {
      return null;
    }
  }

  byte[] getRecordValue(final byte[] key) {
    final var recordOffset = findRecordOffset(key);
    return recordOffset > -1 ? readRecordValue(recordOffset) : null;
  }

  private int getFreeSpaceOffset() {
    return byteBuffer.getInt(FREE_SPACE_OFFSET_OFFSET);
  }

  private void setFreeSpaceOffset(final int freeSpaceOffset) {
    byteBuffer.putInt(FREE_SPACE_OFFSET_OFFSET, freeSpaceOffset);
  }

  private int findRecordOffset(final byte[] wantedKey) {
    final var wantedKeyLength = wantedKey.length;
    final var freeSpaceOffset = getFreeSpaceOffset();
    var currentRecordOffset = DATA_SEGMENT_OFFSET;
    while (currentRecordOffset < freeSpaceOffset) {
      byteBuffer.position(currentRecordOffset);
      final var isDeleted = byteBuffer.get();
      final var keyLength = byteBuffer.getInt();
      final var valueLength = byteBuffer.getInt();
      if (isDeleted == (byte) 0 && keyLength == wantedKeyLength) {
        final var key = new byte[keyLength];
        byteBuffer.position(currentRecordOffset + RECORD_META_DATA_BYTES);
        byteBuffer.get(key);
        if (Arrays.equals(key, wantedKey)) {
          return currentRecordOffset;
        }
      }
      currentRecordOffset += RECORD_META_DATA_BYTES + keyLength + valueLength;
    }
    return -1;
  }

  private byte[] readRecordValue(final int offset) {
    byteBuffer.position(offset + DELETED_FLAG_BYTES);
    final var keyLength = byteBuffer.getInt();
    final var valueLength = byteBuffer.getInt();
    final var value = new byte[valueLength];
    byteBuffer.position(offset + RECORD_META_DATA_BYTES + keyLength);
    byteBuffer.get(value);
    return value;
  }

  private void writeRecord(final int offset, final byte[] key, final byte[] value) {
    byteBuffer.position(offset)
        .put((byte) 0) // is deleted
        .putInt(key.length) // serialized key length
        .putInt(value.length) // serialized value length
        .put(key) // serialized key
        .put(value); // serialized value
  }

}
