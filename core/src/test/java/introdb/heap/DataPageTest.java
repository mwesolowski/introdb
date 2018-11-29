package introdb.heap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.*;

class DataPageTest {
  private static final int pageSize = 4 * 1024;
  private final Serializer serializer = new Serializer(pageSize);
  private DataPage page;


  @BeforeEach
  void setUp() {
    page = DataPage.newPage(1, new byte[pageSize]);
  }

  @Test
  void addRecord() throws IOException {
    // given
    page.addRecord(serializer.serialize("key"), serializer.serialize("value"));

    // when
    var storedValue = page.getRecordValue(serializer.serialize("key"));

    // then
    assertNotNull(storedValue, "record does not exist");
    assertArrayEquals(storedValue, serializer.serialize("value"), "stored value is different than original");
  }

  @Test
  void addAndThenRemoveRecord() throws IOException {
    // given
    page.addRecord(serializer.serialize("key"), serializer.serialize("value"));

    // when
    page.removeRecord(serializer.serialize("key"));
    var storedValue = page.getRecordValue(serializer.serialize("key"));

    // then
    assertNull(storedValue, "record is not deleted");
  }

  @Test
  void addMultipleRecords() throws IOException, ClassNotFoundException {
    // given
    page.addRecord(serializer.serialize("key1"), serializer.serialize("value1"));
    page.addRecord(serializer.serialize("key2"), serializer.serialize("value2"));
    page.addRecord(serializer.serialize("key3"), serializer.serialize("value3"));
    page.addRecord(serializer.serialize("key4"), serializer.serialize("value4"));
    page.addRecord(serializer.serialize("key5"), serializer.serialize("value5"));

    // when
    var storedValue = page.getRecordValue(serializer.serialize("key3"));

    // then
    assertNotNull(storedValue, "record is missing");
    assertArrayEquals(storedValue, serializer.serialize("value3"), "stored value is incorrect");
  }

  @Test
  void overflowPage() throws IOException {
    // given
    var value = new byte[2 * 1024];

    // when
    var firstRecordAdded = page.addRecord(serializer.serialize("key1"), value);
    var secondRecordAdded = page.addRecord(serializer.serialize("key2"), value);

    // then
    assertTrue(firstRecordAdded, "adding first record should succeed");
    assertFalse(secondRecordAdded, "adding second record should fail");
  }

  @Test
  void addRecordTooBigForNewPage() throws IOException {
    // given
    final var value = new byte[4 * 1024];

    // when
    assertThatThrownBy(() -> page.addRecord(serializer.serialize("key"), value))
        .isInstanceOf(IllegalArgumentException.class);
  }
}