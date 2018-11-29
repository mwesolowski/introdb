package introdb.heap.serialization;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SerializationTest {

  @Test
  void jdkSerializationTest() throws ClassNotFoundException, IOException {
    // given
    var serializer = new JdkSerializer(32);
    var entry = "1234567890";

    // when
    var serializedEntry = serializer.serialize(entry);
    var deserializedEntry = serializer.deserialize(serializedEntry);

    // then
    assertEquals(entry, deserializedEntry, "serialized and then deserialized value is different from original");
  }

}