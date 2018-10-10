package com.example.util;

import com.example.watcher.MyWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;

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
     * 创建组，默认断连即删组
     * @param groupName
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void create(String groupName) throws Exception {
        create(groupName,CreateMode.EPHEMERAL);
    }

    /**
     * 指定模式创建zknode或组
     * @param name
     * @param mode
     * @throws Exception
     */
    public void create(String name,CreateMode mode) throws Exception{
        zk.create("/"+name,null, ZooDefs.Ids.OPEN_ACL_UNSAFE,mode);
    }


    /**
     * 创建断连即删组
     * @param name
     * @throws Exception
     */
    public void createEphemeral(String name) throws Exception{
        create(name);
    }

    /**
     * 创建持久化node或组
     * @param name
     * @throws Exception
     */
    public void createPersist(String name) throws Exception{
        create(name,CreateMode.PERSISTENT);
    }

    /**
     * 关闭。
     * @throws InterruptedException
     */
    public void close() throws InterruptedException {
        zk.close();
    }

    /**
     * 判断节点存在
     * @param path
     * @return
     * @throws Exception
     */
    public boolean exists(String path) throws Exception {
        return null!=zk.exists(path,false);
    }

    //列出path下的成员
    public List<String> listChilds(String path) throws Exception{
        return zk.getChildren(path,false);
    }

    //强制删除path及下的成员
    public void forceDelete(String path) throws Exception {
        List<String> children = listChilds(path);
        if(null!=children&&children.size()>0){
            children.forEach(child->{
                try {
                    forceDelete(path+"/"+child);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
        //版本号指定-1代表强制删除。
        zk.delete(path,-1);
    }
}
