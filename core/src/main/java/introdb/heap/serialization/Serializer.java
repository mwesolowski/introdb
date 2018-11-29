package introdb.heap.serialization;

import java.io.IOException;
import java.io.Serializable;

public interface Serializer {

  byte[] serialize(Serializable object) throws IOException;

  Object deserialize(byte[] serializedObject) throws IOException, ClassNotFoundException;

}
