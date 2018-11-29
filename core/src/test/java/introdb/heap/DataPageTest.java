package introdb.heap;

import introdb.heap.serialization.JdkSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.*;

class DataPageTest {
  private DataPage page;

  @BeforeEach
  void setUp() {
    int pageSize = 4 * 1024;
    page = DataPage.newPage(1, new byte[pageSize], new JdkSerializer(pageSize));
  }

  @Test
  void addRecord() throws IOException, ClassNotFoundException {
    // given
    var newRecord = new Record("key", "value");

    // when
    page.addRecord(newRecord);
    var storedRecord = page.getRecord("key");

    // then
    assertNotNull(storedRecord, "record does not exist");
    assertEquals(storedRecord.getValue(), newRecord.getValue(), "stored value is different than original");
  }

  @Test
  void addAndThenRemoveRecord() throws IOException, ClassNotFoundException {
    // given
    page.addRecord(new Record("key", "value"));

    // when
    page.removeRecord("key");
    var storedRecord = page.getRecord("key");

    // then
    assertNull(storedRecord, "record is not deleted");
  }

  @Test
  void addMultipleRecords() throws IOException, ClassNotFoundException {
    // given
    page.addRecord(new Record("key1", "value1"));
    page.addRecord(new Record("key2", "value2"));
    page.addRecord(new Record("key3", "value3"));
    page.addRecord(new Record("key4", "value4"));
    page.addRecord(new Record("key5", "value5"));

    // when
    var record = page.getRecord("key3");

    // then
    assertNotNull(record, "record is missing");
    assertEquals(record.getValue(), "value3", "stored value is incorrect");
  }

  @Test
  void overflowPage() throws IOException {
    // given
    var value = new byte[2 * 1024];

    // when
    var firstRecordAdded = page.addRecord(new Record("key1", value));
    var secondRecordAdded = page.addRecord(new Record("key2", value));

    // then
    assertTrue(firstRecordAdded, "adding first record should succeed");
    assertFalse(secondRecordAdded, "adding second record should fail");
  }

  @Test
  void addRecordTooBigForNewPage() throws IOException {
    // given
    final var value = new byte[4 * 1024];

    // when
    assertThatThrownBy(() -> page.addRecord(new Record("key", value)))
        .isInstanceOf(IllegalArgumentException.class);
  }
}