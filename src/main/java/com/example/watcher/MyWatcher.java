package com.example.watcher;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * 自定义watcher类。
 */
public class MyWatcher implements Watcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyWatcher.class);
    private CountDownLatch latch = new CountDownLatch(1);
    public void process(WatchedEvent event) {
        if(event.getState()==Event.KeeperState.SyncConnected){
            LOGGER.info("客户端连接成功:{}",event);
            latch.countDown();
        }
    }

    public void await() throws InterruptedException {
        latch.await();
    }
}
