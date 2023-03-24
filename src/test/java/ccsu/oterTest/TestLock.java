package ccsu.oterTest;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TestLock {

        private static Lock lock = new ReentrantLock();// 锁对象
        private static Condition canGet = lock.newCondition();
        public int n1 = 1;
        public int n2 = 1;


        public void task() {
            Thread[] t1 = new Thread[n1];
            for (int i = 0; i < n1; i++) {
                R1 r = new R1();
                t1[i] = new Thread(r, "R1999-" + i);
                t1[i].start();
            }
            sleep(2);
            Thread[] t2 = new Thread[n2];
            for (int i = 0; i < n2; i++) {
                R2 r = new R2();
                t2[i] = new Thread(r, "R2000-" + i);
                t2[i].start();
            }
        }


        public static void main(String[] args) {
            TestLock  lock = new TestLock();
            lock.task();
        }






        class R1 implements Runnable {
            public void run() {
                lock.lock();
                try {
                    System.out.println("r1-01\t" + Thread.currentThread().getName());
                    sleep(5);
                    System.out.println("进入等待");
                    canGet.await();
                    System.out.println("离开等待--");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                finally {
                    lock.unlock();
                }
                System.out.println("over");
            }
        }


        class R2 implements Runnable {
            public void run() {
                lock.lock();
                System.out.println("r2-01\t" + Thread.currentThread().getName());
                canGet.signal();
                lock.unlock();
                System.out.println("r2-01\tover");
            }
        }


        public void sleep(int sec) {
            try {
                Thread.sleep(100 * sec);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }






}
