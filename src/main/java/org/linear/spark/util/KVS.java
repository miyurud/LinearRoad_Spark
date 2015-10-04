package org.linear.spark.util;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.CountDownLatch;


public class KVS implements Watcher {
    private static final Charset CHARSET = Charset.forName("UTF-8");
    private static final int SESSION_TIMEOUT = 5000;
    protected ZooKeeper zk;
    private CountDownLatch connectedSignal = new CountDownLatch(1);

    public void write(String path, String value){
        try {
            Stat stat = zk.exists(path, false);
            if (stat == null) {
                zk.create(path, value.getBytes(CHARSET), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            } else {
                zk.setData(path, value.getBytes(CHARSET), -1);
            }
        }catch(InterruptedException e1){
            e1.printStackTrace();
        }catch(KeeperException e2){
            e2.printStackTrace();
        }
    }

    public String read(String path, Watcher watcher){
        try {
            byte[] data = zk.getData(path, watcher, null/*stat*/);
            return new String(data, CHARSET);
        }catch(InterruptedException e1){
            e1.printStackTrace();
        }catch(KeeperException e2){
            e2.printStackTrace();
        }

        return null;
    }

    public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.SyncConnected) {
            connectedSignal.countDown();
        }
    }

    public void connect(String hosts) throws IOException, InterruptedException {
        zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
        connectedSignal.await();
    }

    public void close() throws InterruptedException {
        zk.close();
    }
}
