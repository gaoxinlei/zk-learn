package com.example;

import com.example.util.ZKUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 * 测试用<{@link ZKUtil} 连接zookeeper集群，创建group和断开连接,观察等功能。
 */
public class ZKTest {
    private static final Logger lOGGER = LoggerFactory.getLogger(ZKTest.class);
    private ZKUtil util = new ZKUtil();
    private static final String HOST = "192.168.0.57";
    private static final String GROUP_NAME = "zk_test";
    private static final String NODE_NAME = "zk_node";
    //测试创建一个临时组。
    @Test
    public void testCreateGroup(){
        try {
            createGroup(util);
            //等待过程中使用命令查看创建了组。
            sleepTenSeconds();
            util.close();
        lOGGER.info("成功关闭连接");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void sleepTenSeconds() throws InterruptedException {
        TimeUnit.SECONDS.sleep(10);
    }

    private void createGroup(ZKUtil util) throws Exception {
        lOGGER.info("开始连接主机:{}",HOST);
        util.connect(HOST);
        lOGGER.info("成功连接主机:{}",HOST);
        lOGGER.info("创建组:{}",GROUP_NAME);
        util.create(GROUP_NAME);
        lOGGER.info("成功创建组:{}",GROUP_NAME);
    }

    //测试向某一个组加入一个临时node，若组不存在，建立一个永久组。
    @Test
    public void testJoinGroup() throws Exception {
        createNode(util,GROUP_NAME,NODE_NAME);
        sleepTenSeconds();
        util.close();
    }

    private void createNode(ZKUtil util, String groupName, String nodeName) throws  Exception{
        lOGGER.info("开始连接主机:{}",HOST);
        util.connect(HOST);
        lOGGER.info("成功连接主机:{}",HOST);
        if(!util.exists("/"+groupName)){
            lOGGER.info("创建组:{}",groupName);
            util.createPersist(groupName);
            lOGGER.info("成功创建组:{}",groupName);
        }
        lOGGER.info("创建node:{}",nodeName);
        util.create(groupName+"/"+nodeName);
        lOGGER.info("成功创建node:{}",nodeName);
        util.close();

    }
    //列出成员。
    @Test
    public void testLsMembers() throws Exception{
        util.connect(HOST);
        List<String> children = util.listChilds("/");
        children.forEach(child->{
            lOGGER.info("子节点:{}",child);
        });
        util.close();

    }
    //强制删除path及下面的所有成员
    @Test
    public void testForceDelete() throws Exception{

        util.connect(HOST);
        util.forceDelete("/"+GROUP_NAME);
        util.close();
    }
    //测试监听。
    @Test
    public void testListen() throws  Exception{
        util.connect(HOST);
        String path = "/"+GROUP_NAME+"/"+NODE_NAME;
        if(!util.exists("/"+GROUP_NAME)){
            util.createPersist(GROUP_NAME);
            lOGGER.info("成功创建了节点:{}","/"+GROUP_NAME);
        }
        if(!util.exists(path)){
            util.createPersist(GROUP_NAME+"/"+NODE_NAME);
        }
        util.read(path);
        lOGGER.info("已开始监听，5分钟内每次对节点:{}的更新都将打印",path);
        TimeUnit.SECONDS.sleep(300);
        lOGGER.info("停止监听");
    }

}
