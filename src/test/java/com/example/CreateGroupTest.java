package com.example;

import com.example.util.CreateGroupUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 * 测试用<{@link com.example.util.CreateGroupUtil} 连接zookeeper集群，创建group和断开连接的功能。
 */
public class CreateGroupTest {
    private static final Logger lOGGER = LoggerFactory.getLogger(CreateGroupTest.class);
    private static final String HOST = "192.168.0.57";
    private static final String GROUP_NAME = "zk_test";
    private static final String NODE_NAME = "zk_node";
    //测试创建一个临时组。
    @Test
    public void testCreateGroup(){
        CreateGroupUtil util = new CreateGroupUtil();
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

    private void createGroup(CreateGroupUtil util) throws Exception {
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
        CreateGroupUtil util = new CreateGroupUtil();
        createNode(util,GROUP_NAME,NODE_NAME);
        sleepTenSeconds();
        util.close();
    }

    private void createNode(CreateGroupUtil util,String groupName,String nodeName) throws  Exception{
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
        CreateGroupUtil util = new CreateGroupUtil();
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
        CreateGroupUtil util = new CreateGroupUtil();
        util.connect(HOST);
        util.forceDelete("/"+GROUP_NAME);
        util.close();
    }

}
