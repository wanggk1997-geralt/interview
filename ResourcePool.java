import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ResourcePool {

    public static class Resource implements AutoCloseable {
        private final int id;

        public Resource(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }

        @Override
        public void  close() throws Exception {
            // Simulate closing a database connection
        }
    }

    private final Queue<Resource> available = new ArrayDeque<>();

    private final Set<Resource> inUse = new HashSet<>();

    private final Set<Resource> closing = new HashSet<>();

    private final Set<Resource> closed = new HashSet<>();

    private final Set<Resource> closeFailed = new HashSet<>();

    private final ReentrantLock lock = new ReentrantLock();

    private final Condition notEmpty = lock.newCondition();

    private boolean isShutdown = false;

    public ResourcePool(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException(
                "Capacity must be positive"
            );
        }
        for (int i = 0; i < capacity; i++) {
            available.add(new Resource(i));
        }
    }

    private void closeResource(Resource resource) {
        boolean success = false;
        try {
            resource.close();
            success = true;
        } catch(Exception e) {
           // log
        }
        this.lock.lock();
        try {
            this.closing.remove(resource);
            if (success) closed.add(resource);
            else closeFailed.add(resource);
        } finally {
            
            this.lock.unlock();
        }
    }
    public void shutdown() {
        List<Resource> toClose = new ArrayList<>();
        this.lock.lock();
        try {
            if (isShutdown) return;
            this.isShutdown = true;
            notEmpty.signalAll();
            while (!available.isEmpty()) {
                Resource res = available.remove();
                this.closing.add(res);
                toClose.add(res);
            }
        } finally {
            this.lock.unlock();
        }

        while (!toClose.isEmpty()) {
            Resource res = toClose.remove(0);
            this.closeResource(res);
        }

    }

    public Resource acquire() throws InterruptedException {
        this.lock.lockInterruptibly();
        try {
            while(available.isEmpty() && !this.isShutdown) {
                notEmpty.await();
            }
            if (isShutdown) throw new IllegalStateException("Pool is shutdown");
            Resource res = available.remove();
            this.inUse.add(res);
            return res;
        } finally {
            this.lock.unlock();
        }
    }

    public void release(Resource resource) {
        this.lock.lock();
        boolean shouldClose = false;
        try {
            if (!inUse.remove(resource)) {
                throw new IllegalArgumentException(
                    "Resource is not borrowed from this pool"
                );
            }
            if (!this.isShutdown){
                available.add(resource);
                notEmpty.signal();
            } else {
                this.closing.add(resource);
                shouldClose = true;
            }

        } finally {
            this.lock.unlock();
        }
        if (shouldClose) closeResource(resource);

    }

    public Resource acquire(
        long timeout,
        TimeUnit unit
    ) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        long start = System.nanoTime();
        if (!this.lock.tryLock(nanos, TimeUnit.NANOSECONDS)) {
            return null;
        }
        try {
            long nano = nanos - (System.nanoTime() - start);
            while (available.isEmpty() && !this.isShutdown) {
                if (nano <= 0) {
                    return null;
                }
                nano = this.notEmpty.awaitNanos(nano);
            }
            if (isShutdown) throw new IllegalStateException("Pool is shutdown");
            Resource res = available.remove();
            this.inUse.add(res);
            return res;
        } finally {
            this.lock.unlock();
        }
    }

}
