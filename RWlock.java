// Implement a Writer-Preferred Reader Writer Lock in Java using ReentrantLock and Condition.

// 要求：
// 1. 多个 Reader 可以同时进入。
// 2. Writer 必须独占访问。
// 3. Waiting Writer 可以阻止新 Reader。
// 4. 正确使用 await()、signal() 和 signalAll()。
// 5. 正确处理 InterruptedException。
// 6. 不允许出现 Deadlock 或明显的 Lost Wakeup。
// 暂时不需要实现完整 FIFO，也不要求支持 Reentrancy
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
public class WRLocker {
    int waitingWriters = 0;
    int activeReaders = 0;
    boolean isWriting = false;
    ReentrantLock lock = new ReentrantLock();
    Condition read = lock.newCondition();
    Condition write = lock.newCondition();

    void ReadLock() throws InterruptedException{
        lock.lock();
        try {
            
            while (isWriting || waitingWriters > 0){
                read.await();
            }
            activeReaders ++;
        }
        finally {
            lock.unlock();
        }

    }

    void ReadUnLock(){
        lock.lock();
        try {
            if (activeReaders == 0){
                throw new IllegalStateException();
            }
            activeReaders --;
            if (activeReaders == 0 && waitingWriters > 0){
                write.signal();
            }

        }
        finally {

            lock.unlock();
        }
        
    }

    void WriteLock() throws InterruptedException{
        lock.lock();
        try {
            waitingWriters ++;
            while (isWriting || activeReaders >0){
                write.await();
            }
            isWriting = true;
            
            
        }
        finally {
            waitingWriters --;

            if (waitingWriters == 0 && !isWriting){
                read.signalAll();
            } else if (waitingWriters > 0
                        && !isWriting
                        && activeReaders == 0) {
                    write.signal();
            }
            lock.unlock();
        }
    }

    void WriteUnLock(){
        lock.lock();
        try {
            if (isWriting == false){
                throw Exception();
            }
            isWriting = false;
            if (waitingWriters!=0){
                write.signal();
            }
            else {
                read.signalAll();
            }
        }
        finally {
            lock.unlock();
        }
    }
}