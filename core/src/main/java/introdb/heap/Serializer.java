package introdb.heap;

import java.io.*;

final class Serializer {
  private final int initialBufferCapacity;

  Serializer(final int initialBufferCapacity) {
    this.initialBufferCapacity = initialBufferCapacity;
  }

  byte[] serialize(final Serializable object) throws IOException {
    final var byteArrayOutputStream = new ByteArrayOutputStream(initialBufferCapacity);
    try (final var objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
      objectOutputStream.writeObject(object);
      return byteArrayOutputStream.toByteArray();
    }
  }

  Object deserialize(byte[] serializedObject) throws IOException, ClassNotFoundException {
    final var byteArrayInputStream = new ByteArrayInputStream(serializedObject);
    try (final var objectInputStream = new ObjectInputStream(byteArrayInputStream)) {
      return objectInputStream.readObject();
    }
  }

}
