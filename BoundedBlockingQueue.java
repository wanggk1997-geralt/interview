import java.util.Queue;
import java.util.ArrayDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class BoundedBlockingQueue {

    private final Queue<Integer> queue = new ArrayDeque<>();

    private final int capacity;

    private final ReentrantLock lock = new ReentrantLock();

    private final Condition notFull = lock.newCondition();

    private final Condition notEmpty = lock.newCondition();

    private boolean isShutdown = false;

    public BoundedBlockingQueue(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException(
                "Capacity must be positive"
            );
        }
        this.capacity = capacity;
    }

    public void shutdown() {

        lock.lock();
        try {
            if (this.isShutdown) {
                return;
            }
            this.isShutdown = true;
            notFull.signalAll();
            notEmpty.signalAll();
        } finally {
          lock.unlock();
        }
    }

    public void put(int value) throws InterruptedException {
        lock.lock();

        try{
            if (this.isShutdown) {
                throw new IllegalStateException("Queue is shutdown");
            }
            while (queue.size() == this.capacity && !this.isShutdown) {
                notFull.await();
            }
            if (this.isShutdown) {
                throw new IllegalStateException("Queue is shutdown");
            }
            queue.add(value);
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    public Integer take() throws InterruptedException {
        lock.lock();
        try {
            if (this.isShutdown && queue.isEmpty()) {
                return null;
            }
            while(queue.size() ==0  && !this.isShutdown) {
                notEmpty.await();
            }
            if (queue.isEmpty()) {
                return null;
            }
            Integer res = queue.remove();
            notFull.signal();
            return res;
        } finally {
            lock.unlock();
        }
    }

    public boolean offer(
        int value,
        long timeout,
        TimeUnit unit
    ) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        long start = System.nanoTime();
        
        if (!lock.tryLock(nanos, TimeUnit.NANOSECONDS)) {
            return false;
        }
        try{
            if (this.isShutdown) {
                throw new IllegalStateException("Queue is shutdown");
            }
            nanos -= System.nanoTime() - start;
            while (queue.size() == this.capacity  && !this.isShutdown) {
                if (nanos <= 0) {
                    return false;
                }
                nanos = notFull.awaitNanos(nanos);
            }
            if (this.isShutdown) {
                throw new IllegalStateException("Queue is shutdown");
            }
            queue.add(value);
            notEmpty.signal();
            return true;
        } finally {
            lock.unlock();
        }
    }
}