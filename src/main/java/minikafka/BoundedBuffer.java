package minikafka;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

//Blocking bounded queue (ring buffer) built on a lock + two conditions
public class BoundedBuffer<T> {
    private final Object[] slots;
    private int head, tail, size;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notFull = lock.newCondition();
    private final Condition notEmpty = lock.newCondition();

    public BoundedBuffer(int capacity) { slots = new Object[capacity]; }

    //Producer

    public void put(T item) throws InterruptedException {
        lock.lock();
        try {
            //check if buffer is empty, if empty put consumer on sleep
            while (size == slots.length) notFull.await();
            //If empty then add the item
            slots[tail] = item;
            //Increase the index
            tail = (tail + 1) % slots.length;
            //Update the buffer filled value
            size++;
            //signal the consumer that buffer is no more empty and now it can consume
            notEmpty.signal();
        }finally
        {
            lock.unlock();
        }
    }

    //Consumer

    @SuppressWarnings("unchecked")
    public T take() throws InterruptedException {
        lock.lock();
        try {
            //If empty then wait
            while (size == 0) notEmpty.await();
            T item = (T) slots[head];
            slots[head] = null;
            head = (head + 1) % slots.length;
            //consume and reduce the size
            size--;
            //acknowledge the buffer that consumed
            notFull.signal();
            return item;
        }
        finally
        {
            lock.unlock();
        }
    }
}
