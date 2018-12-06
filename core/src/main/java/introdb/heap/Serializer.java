package introdb.heap;

import java.io.*;

final class Serializer {
  private final ByteArrayOutputStream byteArrayOutputStream;
  private final ObjectOutputStream objectOutputStream;
  private final byte[] serializationMagicAndVersion;

  Serializer(final int initialBufferCapacity) throws IOException {
    this.byteArrayOutputStream = new ByteArrayOutputStream(initialBufferCapacity);
    this.objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
    this.serializationMagicAndVersion = byteArrayOutputStream.toByteArray();
  }

  byte[] serialize(final Serializable object) throws IOException {
    resetOutputBuffers();
    objectOutputStream.writeObject(object);
    return byteArrayOutputStream.toByteArray();
  }

  private void resetOutputBuffers() throws IOException {
    byteArrayOutputStream.reset();
    byteArrayOutputStream.writeBytes(serializationMagicAndVersion);
    objectOutputStream.reset();
  }

  Object deserialize(final byte[] serializedObject) throws IOException, ClassNotFoundException {
    final var byteArrayInputStream = new ByteArrayInputStream(serializedObject);
    try (final var objectInputStream = new ObjectInputStream(byteArrayInputStream)) {
      return objectInputStream.readObject();
    }
  }

}
