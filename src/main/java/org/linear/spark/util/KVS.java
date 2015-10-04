/**
 Copyright 2015 Miyuru Dayarathna

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

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
