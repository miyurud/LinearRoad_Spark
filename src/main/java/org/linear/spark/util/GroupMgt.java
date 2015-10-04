package org.linear.spark.util;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * Created by miyurud on 9/29/15.
 */
public class GroupMgt implements Watcher {
    private static final int SESSION_TIMEOUT = 5000;
    private ZooKeeper zk;
    private CountDownLatch connectedSignal = new CountDownLatch(1);

    public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.SyncConnected) {
            connectedSignal.countDown();
        }
    }

    public void create(String groupName) {
        try {
            String path = "/" + groupName;
            String createdPath = zk.create(path, null/*data*/, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println("Created " + createdPath);
        }catch(InterruptedException e1){
            e1.printStackTrace();
        }catch(KeeperException e2){
            e2.printStackTrace();
        }
    }

    public void connect(String hosts) {
        try {
            zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
            connectedSignal.await();
        }catch(IOException e1){
            e1.printStackTrace();
        }catch(InterruptedException e2){
            e2.printStackTrace();
        }
    }

    public void close() throws InterruptedException {
        zk.close();
    }
}
