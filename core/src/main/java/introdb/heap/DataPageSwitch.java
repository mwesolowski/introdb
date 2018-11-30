package introdb.heap;

import java.nio.MappedByteBuffer;
import java.util.Arrays;

final class DataPageSwitch {
  private final MappedByteBuffer mappedByteBuffer;
  private final int pageSize;
  private final byte[] selectedPageData;
  private int selectedPageOffset;
  private DataPageOperator pageOperator;

  DataPageSwitch(final MappedByteBuffer mappedByteBuffer, final int pageSize) {
    this.mappedByteBuffer = mappedByteBuffer;
    this.pageSize = pageSize;
    this.selectedPageData = new byte[pageSize];
    this.pageOperator = new DataPageOperator(selectedPageData);
  }

  void loadPage(final int pageNumber) {
    selectedPageOffset = pageSize * pageNumber;
    mappedByteBuffer.position(selectedPageOffset).get(selectedPageData);
    pageOperator.initializeExistingPage();
  }

  void createAndSelectPage(final int pageNumber) {
    selectedPageOffset = pageSize * pageNumber;
    Arrays.fill(selectedPageData, (byte) 0);
    pageOperator.initializeNewPage();
    saveSelectedPage();
  }

  byte[] findInSelectedPage(final byte[] key) {
    return pageOperator.getRecordValue(key);
  }

  boolean addToSelectedPage(final byte[] key, final byte[] value) {
    final boolean added = pageOperator.addRecord(key, value);
    if (added) {
      saveSelectedPage();
    }
    return added;
  }

  byte[] removeFromSelectedPage(final byte[] key) {
    final var removedValue = pageOperator.removeRecord(key);
    if (removedValue != null) {
      saveSelectedPage();
    }
    return removedValue;
  }

  private void saveSelectedPage() {
    mappedByteBuffer.position(selectedPageOffset).put(selectedPageData);
  }

}
