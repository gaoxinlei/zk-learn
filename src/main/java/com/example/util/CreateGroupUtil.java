package com.example.util;

import com.example.watcher.MyWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

/**
 * 先connect,再create，再close。
 */
public class CreateGroupUtil {
    private static final int SESSION_TIME_OUT = 5000;
    private ZooKeeper zk;

    /**
     * 连接。
     * @param host
     * @throws Exception
     */
    public void connect(String host) throws  Exception{
        MyWatcher watcher = new MyWatcher();
        zk = new ZooKeeper(host,SESSION_TIME_OUT,watcher);
        watcher.await();
    }

    /**
     * 创建断连即删组
     * @param groupName
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void create(String groupName) throws KeeperException, InterruptedException {
        zk.create("/"+groupName,null, ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);
    }

    /**
     * 关闭。
     * @throws InterruptedException
     */
    public void close() throws InterruptedException {
        zk.close();
    }

}
