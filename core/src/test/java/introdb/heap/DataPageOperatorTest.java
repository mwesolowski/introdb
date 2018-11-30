package introdb.heap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.*;

class DataPageOperatorTest {
  private static final int pageSize = 4 * 1024;
  private DataPageOperator pageOperator;
  private Serializer serializer;


  @BeforeEach
  void setUp() throws IOException {
    pageOperator = new DataPageOperator(new byte[pageSize]);
    pageOperator.initializeNewPage();
    serializer = new Serializer(pageSize);
  }

  @Test
  void addRecord() throws IOException {
    // given
    pageOperator.addRecord(serializer.serialize("key"), serializer.serialize("value"));

    // when
    var storedValue = pageOperator.getRecordValue(serializer.serialize("key"));

    // then
    assertNotNull(storedValue, "record does not exist");
    assertArrayEquals(storedValue, serializer.serialize("value"), "stored value is different than original");
  }

  @Test
  void addAndThenRemoveRecord() throws IOException {
    // given
    pageOperator.addRecord(serializer.serialize("key"), serializer.serialize("value"));

    // when
    pageOperator.removeRecord(serializer.serialize("key"));
    var storedValue = pageOperator.getRecordValue(serializer.serialize("key"));

    // then
    assertNull(storedValue, "record is not deleted");
  }

  @Test
  void addMultipleRecords() throws IOException {
    // given
    pageOperator.addRecord(serializer.serialize("key1"), serializer.serialize("value1"));
    pageOperator.addRecord(serializer.serialize("key2"), serializer.serialize("value2"));
    pageOperator.addRecord(serializer.serialize("key3"), serializer.serialize("value3"));
    pageOperator.addRecord(serializer.serialize("key4"), serializer.serialize("value4"));
    pageOperator.addRecord(serializer.serialize("key5"), serializer.serialize("value5"));

    // when
    var storedValue = pageOperator.getRecordValue(serializer.serialize("key3"));

    // then
    assertNotNull(storedValue, "record is missing");
    assertArrayEquals(storedValue, serializer.serialize("value3"), "stored value is incorrect");
  }

  @Test
  void overflowPage() throws IOException {
    // given
    var value = new byte[2 * 1024];

    // when
    var firstRecordAdded = pageOperator.addRecord(serializer.serialize("key1"), value);
    var secondRecordAdded = pageOperator.addRecord(serializer.serialize("key2"), value);

    // then
    assertTrue(firstRecordAdded, "adding first record should succeed");
    assertFalse(secondRecordAdded, "adding second record should fail");
  }

  @Test
  void addRecordTooBigForNewPage() {
    // given
    final var value = new byte[4 * 1024];

    // when
    assertThatThrownBy(() -> pageOperator.addRecord(serializer.serialize("key"), value))
        .isInstanceOf(IllegalArgumentException.class);
  }
}