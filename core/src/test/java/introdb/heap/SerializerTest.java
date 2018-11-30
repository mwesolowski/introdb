package introdb.heap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SerializerTest {
  private Serializer serializer;

  @BeforeEach
  void setUp() throws IOException {
    serializer = new Serializer(4 * 1024);
  }

  @Test
  void singleSerializationTest() throws ClassNotFoundException, IOException {
    // given
    var entry = "1234567890";

    // when
    var serializedEntry = serializer.serialize(entry);
    var deserializedEntry = serializer.deserialize(serializedEntry);

    // then
    assertEquals(entry, deserializedEntry, "serialized and then deserialized value is different from original");
  }

  @Test
  void multipleSerializationTest() throws ClassNotFoundException, IOException {
    // given
    var entry1 = "1234567890";
    var entry2 = 123;

    // when
    var serializedEntry1 = serializer.serialize(entry1);
    var serializedEntry2 = serializer.serialize(entry2);
    var deserializedEntry1 = serializer.deserialize(serializedEntry1);
    var deserializedEntry2 = serializer.deserialize(serializedEntry2);

    // then
    assertEquals(entry1, deserializedEntry1, "first serialized and then deserialized value is different from original");
    assertEquals(entry2, deserializedEntry2, "second serialized and then deserialized value is different from original");
  }

}