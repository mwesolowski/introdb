package introdb.heap.pool;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class ObjectPool<T> {

  private final ObjectFactory<T> fcty;
  private final ObjectValidator<T> validator;
  private final int maxPoolSize;
  private final ArrayBlockingQueue<T> queue;
  private final AtomicInteger atomicCreatedCount;

  public ObjectPool(ObjectFactory<T> fcty, ObjectValidator<T> validator) {
    this(fcty, validator, 25);
  }

  public ObjectPool(ObjectFactory<T> fcty, ObjectValidator<T> validator, int maxPoolSize) {
    this.fcty = fcty;
    this.validator = validator;
    this.maxPoolSize = maxPoolSize;
    this.queue = new ArrayBlockingQueue<>(maxPoolSize);
    this.atomicCreatedCount = new AtomicInteger(0);
  }

  /**
   * When there is object in pool returns completed future,
   * if not, future will be completed when object is
   * returned to the pool.
   *
   * @return
   */
  public CompletableFuture<T> borrowObject() {
    final var obj = queue.poll();
    if (obj != null) {
      return CompletableFuture.completedFuture(obj);
    }
    var createdCount = atomicCreatedCount.get();
    while (createdCount < maxPoolSize) {
      if (atomicCreatedCount.compareAndSet(createdCount, createdCount + 1)) {
        return CompletableFuture.completedFuture(fcty.create());
      }
      createdCount = atomicCreatedCount.get();
    }
    return CompletableFuture.supplyAsync(this::takeFromQueueWithoutUncheckedException);
  }

  private T takeFromQueueWithoutUncheckedException() {
    try {
      return queue.take();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public void returnObject(T object) {
    queue.add(object);
  }

  public void shutdown() throws InterruptedException {
  }

  public int getPoolSize() {
    return atomicCreatedCount.get();
  }

  public int getInUse() {
    return atomicCreatedCount.get() - queue.size();
  }

}
