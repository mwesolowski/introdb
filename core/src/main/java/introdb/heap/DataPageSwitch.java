package introdb.heap;

import java.nio.MappedByteBuffer;
import java.util.Arrays;

final class DataPageSwitch {
  private final MappedByteBuffer mappedByteBuffer;
  private final int pageSize;
  private final byte[] byteBuffer;
  private int selectedPageOffset;
  private DataPage selectedPage;

  DataPageSwitch(final MappedByteBuffer mappedByteBuffer, final int pageSize) {
    this.mappedByteBuffer = mappedByteBuffer;
    this.pageSize = pageSize;
    this.byteBuffer = new byte[pageSize];
  }

  void loadPage(final int pageNumber) {
    selectedPageOffset = pageSize * pageNumber;
    mappedByteBuffer.position(selectedPageOffset).get(byteBuffer);
    selectedPage = DataPage.existingPage(byteBuffer);
  }

  void createAndSelectPage(final int pageNumber) {
    selectedPageOffset = pageSize * pageNumber;
    Arrays.fill(byteBuffer, (byte) 0);
    selectedPage = DataPage.newPage(byteBuffer);
    saveSelectedPage();
  }

  byte[] findInSelectedPage(final byte[] key) {
    return selectedPage.getRecordValue(key);
  }

  boolean addToSelectedPage(final byte[] key, final byte[] value) {
    final boolean added = selectedPage.addRecord(key, value);
    if (added) {
      saveSelectedPage();
    }
    return added;
  }

  byte[] removeFromSelectedPage(final byte[] key) {
    final var removedValue = selectedPage.removeRecord(key);
    if (removedValue != null) {
      saveSelectedPage();
    }
    return removedValue;
  }

  private void saveSelectedPage() {
    mappedByteBuffer.position(selectedPageOffset).put(byteBuffer);
  }

}
