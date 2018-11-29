package introdb.heap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SerializerTest {
  private Serializer serializer;

  @BeforeEach
  void setUp() {
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

}