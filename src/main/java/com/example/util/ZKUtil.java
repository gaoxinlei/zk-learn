package com.example.util;

import com.example.watcher.MyWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 先connect,再create，再close。
 */
public class ZKUtil {
    private static final int SESSION_TIME_OUT = 5000;
    private static final Logger LOGGER = LoggerFactory.getLogger(ZKUtil.class);
    private ZooKeeper zk;

    /**
     * 连接。
     *
     * @param host
     * @throws Exception
     */
    public void connect(String host) throws Exception {
        MyWatcher watcher = new MyWatcher();
        zk = new ZooKeeper(host, SESSION_TIME_OUT, watcher);
        watcher.await();
    }

    /**
     * 创建组，默认断连即删组
     *
     * @param groupName
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void create(String groupName) throws Exception {
        create(groupName, CreateMode.EPHEMERAL);
    }

    /**
     * 指定模式创建zknode或组
     *
     * @param name
     * @param mode
     * @throws Exception
     */
    public void create(String name, CreateMode mode) throws Exception {
        zk.create("/" + name, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
    }


    /**
     * 创建断连即删组
     *
     * @param name
     * @throws Exception
     */
    public void createEphemeral(String name) throws Exception {
        create(name);
    }

    /**
     * 创建持久化node或组
     *
     * @param name
     * @throws Exception
     */
    public void createPersist(String name) throws Exception {
        create(name, CreateMode.PERSISTENT);
    }

    /**
     * 关闭。
     *
     * @throws InterruptedException
     */
    public void close() throws InterruptedException {
        zk.close();
    }

    /**
     * 判断节点存在
     *
     * @param path
     * @return
     * @throws Exception
     */
    public boolean exists(String path) throws Exception {
        return null != zk.exists(path, false);
    }

    //列出path下的成员
    public List<String> listChilds(String path) throws Exception {
        return zk.getChildren(path, false);
    }

    //强制删除path及下的成员
    public void forceDelete(String path) throws Exception {
        List<String> children = listChilds(path);
        if (null != children && children.size() > 0) {
            children.forEach(child -> {
                try {
                    forceDelete(path + "/" + child);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
        //版本号指定-1代表强制删除。
        zk.delete(path, -1);
    }

    //监听方式读取数据
    public String read(String path) throws Exception {
        Stat stat = zk.exists(path, false);
        return pullData(path, stat);
    }

    private String pullData(String path, Stat stat) throws KeeperException, InterruptedException {
        byte[] data = zk.getData(path, this::watch, stat);
        String message = data == null ? "" : new String(data);
        LOGGER.info("节点：{}下数据内容：{}", path, message);
        return message;
    }

    //观察事件
    public void watch(WatchedEvent event)  {

        if (event.getType() == Watcher.Event.EventType.NodeDataChanged) {
            LOGGER.info("监听到节点:{}下的数据变更", event.getPath());
        }
        //继续监听直到主线程结束。
        try {
            pullData(event.getPath(),zk.exists(event.getPath(),false));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
