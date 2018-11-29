package introdb.heap;

import java.io.Serializable;

final class Record implements Serializable {
  private final Serializable key;
  private final Serializable value;
  private boolean deleted;

  Record(Serializable key, Serializable value) {
    this.key = key;
    this.value = value;
    this.deleted = false;
  }

  Serializable getKey() {
    return key;
  }

  Serializable getValue() {
    return value;
  }

  boolean isDeleted() {
    return deleted;
  }

  void markAsDeleted() {
    this.deleted = true;
  }

}
