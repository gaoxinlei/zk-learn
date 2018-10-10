package com.example;

import com.example.util.CreateGroupUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 *
 * 测试用<{@link com.example.util.CreateGroupUtil} 连接zookeeper集群，创建group和断开连接的功能。
 */
public class CreateGroupTest {
    private static final Logger lOGGER = LoggerFactory.getLogger(CreateGroupTest.class);
    private static final String HOST = "192.168.0.57";
    private static final String GROUP_NAME = "zk_test";
    @Test
    public void testCreateGroup(){
        CreateGroupUtil util = new CreateGroupUtil();
        try {
            lOGGER.info("开始连接主机:{}",HOST);
            util.connect(HOST);
            lOGGER.info("成功连接主机:{}",HOST);
            lOGGER.info("创建组:{}",GROUP_NAME);
            util.create(GROUP_NAME);
            lOGGER.info("成功创建组:{}",GROUP_NAME);
            TimeUnit.SECONDS.sleep(10);
            util.close();
            lOGGER.info("成功关闭连接");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
