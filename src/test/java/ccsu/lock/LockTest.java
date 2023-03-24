package ccsu.lock;

import com.ccsu.vm.LockManager;
import org.junit.Test;

public class LockTest {
    @Test
    public void Test01() throws InterruptedException {
        System.out.println("??");
        LockManager lockManager=new LockManager();
        LockThread lockThread1 = new LockThread(lockManager, 1, 1);
        LockThread lockThread2 = new LockThread(lockManager, 2, 2);
        LockThread lockThread3 = new LockThread(lockManager, 3, 3);
        LockThread lockThread4 = new LockThread(lockManager, 4, 4);

        LockThread lockThread5 = new LockThread(lockManager, 1, 2);
        LockThread lockThread6 = new LockThread(lockManager, 2, 3);
        LockThread lockThread7 = new LockThread(lockManager, 3, 4);
        LockThread lockThread8 = new LockThread(lockManager, 4, 1);

        Thread thread1 = new Thread(lockThread1);
        thread1.start();
        Thread.sleep(300);

        Thread thread2 = new Thread(lockThread2);
        thread2.start();
        Thread.sleep(300);

        Thread thread3 = new Thread(lockThread3);
        thread3.start();
        Thread.sleep(300);

        Thread thread4 = new Thread(lockThread4);
        thread4.start();
        Thread.sleep(300);



        Thread thread5 = new Thread(lockThread5);
        thread5.start();
        Thread.sleep(300);

        Thread thread6 = new Thread(lockThread6);
        thread6.start();
        Thread.sleep(300);

        Thread thread7 = new Thread(lockThread7);
        thread7.start();
        Thread.sleep(300);

        Thread thread8 = new Thread(lockThread8);
        thread8.start();
        Thread.sleep(300);


        try {
            Thread.sleep(2000);
//            lockManager.remove(1);
            thread1.join();
            thread2.join();
            thread3.join();
            thread4.join();
            thread5.join();
            thread6.join();
            thread7.join();
            thread8.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }


    }


    static class LockThread implements Runnable{
        LockManager lockManager;
        long xid;
        long uid;


        public LockThread(LockManager lockManager,long xid,long uid) {
            this.lockManager = lockManager;
            this.xid=xid;
            this.uid=uid;
        }

        @Override
        public void run() {
            try {

                lockManager.add(xid,uid);
                System.out.println(xid+"获取了"+uid);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
