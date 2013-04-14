import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;

import java.util.concurrent.CountDownLatch;
import java.util.List;
import java.io.IOException;

public class ZkConnector implements Watcher {

    // ZooKeeper Object
    ZooKeeper zooKeeper;

    // To block any operation until ZooKeeper is connected. It's initialized
    // with count 1, that is, ZooKeeper connect state.
    CountDownLatch connectedSignal = new CountDownLatch(1);
    
    protected static final List<ACL> acl = Ids.OPEN_ACL_UNSAFE;

    /**
     * Connects to ZooKeeper servers specified by hosts.
     * @param hosts ZooKeeper service hosts
     */
    public void connect(String hosts) throws IOException, InterruptedException {
        zooKeeper = new ZooKeeper(
                hosts, // ZooKeeper service hosts
                5000,// Session timeout in milliseconds
                this); // watches -- this class implements Watcher; events are handled by the process method
	    connectedSignal.await();
    }

    /**
     * Closes connection with ZooKeeper
     */
    public void close() throws InterruptedException {
	    zooKeeper.close();
    }

    /**
     * @return the zooKeeper
     */
    public ZooKeeper getZooKeeper() throws IllegalStateException {
        // Verify ZooKeeper's validity
        if (zooKeeper == null || !zooKeeper.getState().equals(States.CONNECTED)) {
	        throw new IllegalStateException ("ZooKeeper is not connected.");
        }
        return zooKeeper;
    }

    public void process(WatchedEvent event) {
        // check if event is for connection done; if so, unblock main thread blocked in connect
        if (event.getState() == KeeperState.SyncConnected) {
            connectedSignal.countDown();
        } else { // It should not get any unhandeled events
        	System.out.println("zkConnector: unhandeled watch event: " + event.toString());
        }
    }
}

