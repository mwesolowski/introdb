package introdb.heap;

import java.nio.ByteBuffer;
import java.util.Arrays;

class Record {
  private static final int BOOLEAN_FLAG_BYTES = Byte.BYTES;
  private static final int SIZE_FIELD_BYTES = Integer.BYTES;

  private static final int IS_NOT_DELETED_FLAG_OFFSET = 0;
  private static final int KEY_LENGTH_OFFSET = IS_NOT_DELETED_FLAG_OFFSET + BOOLEAN_FLAG_BYTES;
  private static final int VALUE_LENGTH_OFFSET = KEY_LENGTH_OFFSET + SIZE_FIELD_BYTES;
  private static final int KEY_OFFSET = VALUE_LENGTH_OFFSET + SIZE_FIELD_BYTES;

  private ByteBuffer byteBuffer;
  private int offset;

  static int recordLengthFor(final byte[] key, final byte[] value) {
    return KEY_OFFSET + key.length + value.length;
  }

  void createNewRecord(final ByteBuffer byteBuffer, final int offset, final byte[] key, final byte[] value) {
    byteBuffer.position(offset)
        .put((byte) 0)
        .putInt(key.length)
        .putInt(value.length)
        .put(key)
        .put(value);
    loadRecord(byteBuffer, offset);
  }

  void loadRecord(final ByteBuffer byteBuffer, final int offset) {
    this.byteBuffer = byteBuffer;
    this.offset = offset;
  }

  boolean isNotDeleted() {
    return byteBuffer.position(offset).get() == (byte) 0;
  }

  boolean hasKey(final byte[] key) {
    final var keyLength = byteBuffer.position(offset + KEY_LENGTH_OFFSET).getInt();
    if (keyLength == key.length) {
      var recordKey = new byte[keyLength];
      byteBuffer.position(offset + KEY_OFFSET).get(recordKey);
      return Arrays.equals(recordKey, key);
    }
    return false;
  }

  byte[] getValue() {
    final var keyLength = byteBuffer.position(offset + KEY_LENGTH_OFFSET).getInt();
    final var valueLength = byteBuffer.getInt();
    final var recordValue = new byte[valueLength];
    byteBuffer.position(offset + KEY_OFFSET + keyLength).get(recordValue);
    return recordValue;
  }

  void markAsDeleted() {
    byteBuffer.position(offset + IS_NOT_DELETED_FLAG_OFFSET).put((byte) 1);
  }

  int getLength() {
    final var keyLength = byteBuffer.position(offset + KEY_LENGTH_OFFSET).getInt();
    final var valueLength = byteBuffer.getInt();
    return KEY_OFFSET + keyLength + valueLength;
  }

}
