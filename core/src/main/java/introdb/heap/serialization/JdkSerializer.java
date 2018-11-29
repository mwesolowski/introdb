package introdb.heap.serialization;

import java.io.*;

final public class JdkSerializer implements Serializer {
  private final int initialBufferCapacity;

  public JdkSerializer(final int initialBufferCapacity) {
    this.initialBufferCapacity = initialBufferCapacity;
  }

  @Override
  public byte[] serialize(final Serializable object) throws IOException {
    final var byteArrayOutputStream = new ByteArrayOutputStream(initialBufferCapacity);
    try (final var objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
      objectOutputStream.writeObject(object);
      return byteArrayOutputStream.toByteArray();
    }
  }

  @Override
  public Object deserialize(byte[] serializedObject) throws IOException, ClassNotFoundException {
    final var byteArrayInputStream = new ByteArrayInputStream(serializedObject);
    try (final var objectInputStream = new ObjectInputStream(byteArrayInputStream)) {
      return objectInputStream.readObject();
    }
  }

}
