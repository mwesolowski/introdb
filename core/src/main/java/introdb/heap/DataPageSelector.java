package introdb.heap;

import java.nio.MappedByteBuffer;
import java.util.Arrays;

final class DataPageSelector {
  private final MappedByteBuffer mappedByteBuffer;
  private final int pageSize;
  private final byte[] selectedPageData;
  private int selectedPageOffset;
  private DataPageOperator pageOperator;

  DataPageSelector(final MappedByteBuffer mappedByteBuffer, final int pageSize) {
    this.mappedByteBuffer = mappedByteBuffer;
    this.pageSize = pageSize;
    this.selectedPageData = new byte[pageSize];
    this.pageOperator = new DataPageOperator(selectedPageData);
  }

  void selectPage(final int pageNumber) {
    selectedPageOffset = pageSize * pageNumber;
    loadSelectedPageDataFromMemoryMappedFile();
    pageOperator.initializeExistingPage();
  }

  void createAndSelectPage(final int pageNumber) {
    Arrays.fill(selectedPageData, (byte) 0);
    selectedPageOffset = pageSize * pageNumber;
    pageOperator.initializeNewPage();
    saveSelectedPageDataToMemoryMappedFile();
  }

  byte[] findInSelectedPage(final byte[] key) {
    return pageOperator.getRecordValue(key);
  }

  boolean addToSelectedPage(final byte[] key, final byte[] value) {
    final boolean added = pageOperator.addRecord(key, value);
    if (added) {
      saveSelectedPageDataToMemoryMappedFile();
    }
    return added;
  }

  byte[] removeFromSelectedPage(final byte[] key) {
    final var removedValue = pageOperator.removeRecord(key);
    if (removedValue != null) {
      saveSelectedPageDataToMemoryMappedFile();
    }
    return removedValue;
  }

  private void loadSelectedPageDataFromMemoryMappedFile() {
    mappedByteBuffer.position(selectedPageOffset).get(selectedPageData);
  }

  private void saveSelectedPageDataToMemoryMappedFile() {
    mappedByteBuffer.position(selectedPageOffset).put(selectedPageData);
  }

}
