import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;

/**
 * Class that uses zookeeper's functionality to create higher level constructs like distributed locking.
 * Multiple thread must not use one instanc of this class. Will cause trouble with internal state maintenance
 * @author Hasan Imam
 */

public class ZkSync implements Watcher {
	/** A synchronization aid for signalling release of blocking calls. All function using this should instantiate one for itself. It should also clean up once it is done with it */
	private CountDownLatch isMyTurn = null;
	
	/** A path that is currently being watched */
	private String watchPath = null;	
	
	/** Associated zookeeper object */
	private final ZooKeeper zk;
	
	/** List of locks nodes (their path) that are being held by this synchronizer */
	private List<String> lockList = new LinkedList<String>();
	
	/** 
	 * Watcher for implementing locking and related event handling 
	 * @param zookeeper Zookeeper object that has already been connected 
	 */
	public ZkSync(ZooKeeper zookeeper) {
		zk = zookeeper;
	}
	
	/**
	 * Same functionality as the other overridden method. See that one's description. This function simply derives the arguments for the other method and calls it
	 * @param lockPath The node on which we want to get the lock on
	 * @return True if the lock was successful
	 */
	public boolean getLock(String lockPath) throws InterruptedException, KeeperException {
		assert(lockPath != null && lockPath.length() > 1 && lockPath.contains("/"));
		String lockPrefix = lockPath.substring(lockPath.lastIndexOf('/') + 1, lockPath.length());
		String lockDir = lockPath.substring(0, lockPath.length() - lockPrefix.length());
		return getLock(lockDir, lockPrefix);
	}
	
	/**
	 * Same functionality as the other overridden method. See that one's description. This function simply derives the arguments for the other method and calls it
	 * @param lockPath The node that we want to unlock
	 * @return True if the unlocking was successful
	 */
	public boolean unLock(String lockPath) throws InterruptedException, KeeperException {
		assert(lockPath != null && lockPath.length() > 1 && lockPath.contains("/"));
		String lockPrefix = lockPath.substring(lockPath.lastIndexOf('/') + 1, lockPath.length());
		String lockDir = lockPath.substring(0, lockPath.length() - lockPrefix.length());
		return unLock(lockDir, lockPrefix);
	}
	
	/**
	 * Gets a lock on an entity defined by lockPathPrefix. For example if you want a lock on the node: /Clients/Hasan, give a prefix like 'Hasan_lock-' and path should be '/Clients'. All entities that want to achive the lock must agree to provide the same prefix entry while calling this function.
	 * Function will block until this node gets the lock. It's very important to use this lock object in only one thread.
	 * @param lockDir The directory where the lock node will be created. Operation fails if given path is invalid, or this client does not have authority to create nodes in, or is Ephemeral (Ephemeral nodes cannot have child nodes)
	 * @param lockPrefix The same prefix needs to be provided by all entities that are trying to get the same lock. It's called prefix since it creates sequential nodes named path+prefix to implement the locking.
	 * @return True if the lock was successful
	 */
	public boolean getLock(String lockDir, String lockPrefix) throws InterruptedException, KeeperException{
		if (lockDir == null || lockDir == null || zk == null || !zk.getState().equals(States.CONNECTED)) {
			return false;
		}
		String lockPath = null;
		try {
			char seqPrefix = lockPrefix.charAt(lockPrefix.length() - 1);
			// Create my lock node
			lockPath = zk.create(lockDir + "/" + lockPrefix, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
			// Check if there is any previous lock holder
			boolean myTurn = true;
			int mySequence = Integer.parseInt(lockPath.substring(lockPath.lastIndexOf(seqPrefix) + 1));
			do { // Keeps checking until there is no previous lock holder
				myTurn = true; // Reset for this iteration
				List<String> children = zk.getChildren(lockDir, false);
				int previousSequence = -1; // Immediate previous holder's seq
				String prevSeqChild = null; // Immediate previous holder's node name (for the sake of setting a watch later)
				for (String child : children) {
					if (child.contains(lockPrefix) && child.equals(lockPrefix) == false) { // Checks if this is a lock on the same node as ours
						int childSequence = Integer.parseInt(child.substring(child.lastIndexOf(seqPrefix) + 1)); // Get this lock node's seq number
						if (childSequence < mySequence) { // Someone has lock sequence number lower than mine.
							myTurn = false;  // Not my turn since lower # guy goes first
							if (childSequence > previousSequence) { // Is this seq number closer to my seq than the previous guy's seq (trying to find the last immediate lock holder)
								previousSequence = childSequence;
								prevSeqChild = child;
							}
						}
					}
				} // At this point found out immediate previous lock holder (if any)
				
				if (!myTurn) {
					assert(previousSequence != -1 && previousSequence < mySequence && prevSeqChild != null);
					// set a watch on the immediate previous lock holder
					Stat stat = zk.exists(lockDir + "/" + prevSeqChild, this);
					if (stat != null) { // Incase the lock has been released since the last time we looked
						// Block thread
						isMyTurn = new CountDownLatch(1);
						watchPath = lockDir + "/" + prevSeqChild;
						//parentWatchPath = lockDir;
						isMyTurn.await(); // Released when the event processor detects that the previous lock holder has released its lock
						isMyTurn = null; // Once unlocked, this counter has no utility
					} // Else case implies lock does not exist. Go back to checking
				}
			} while (!myTurn);
		} catch (IndexOutOfBoundsException e) {
			System.err.println("ZkSync: Invalid prefix and/or directory argument (IndexOutOfBound)");
			return false;
		} catch (KeeperException.NoNodeException e) {
			System.err.println("ZkSync: Invalid prefix and/or directory argument (NoNode)");
			return false;
		} finally { // Clean up internal bookkeeping
			assert(watchPath == null);
			assert(isMyTurn == null);
		}
		// Successfully got the lock. Add to the list of lock nodes
		assert(lockPath != null);
		if (lockList.add(lockPath) == false) {
			assert(true);
		} 
		return true;
	}

	/**
	 * Releases the lock that was acquired before. The arguments given should be the same as the ones given to get the lock while calling getLock(...) function
	 * @param lockDir The directory where the lock node was created. 
	 * @param lockPrefix The same prefix used to get this lock
	 * @return True if the unlocking was successful. False if the lock doesnt exist or had other failures
	 */
	public boolean unLock(String lockDir, String lockPrefix) throws InterruptedException, KeeperException{
		if (lockDir == null || lockDir == null || zk == null || !zk.getState().equals(States.CONNECTED)) {
			return false;
		}
		try {
			// Delete my lock node
			try {
				for (Iterator<String> iterator = lockList.iterator(); iterator.hasNext();) {
					String lockNodePath = iterator.next();
					if (lockNodePath.startsWith(lockDir + "/" + lockPrefix)) { // Found the correct lock
						// Delete lock
						zk.delete(lockNodePath, -1);
						// Delete lock's node path from list
						iterator.remove();
						return true;
					}
				}
			} catch (KeeperException.NoNodeException e) {
				// Lock node does not exist. Calling unlock without having a lock
				return false;
			}
		} catch (IndexOutOfBoundsException e) {
			System.err.println("ZkSync: Invalid prefix and/or directory argument (IndexOutOfBound)");
			return false;
		} catch (KeeperException.NoNodeException e) {
			System.err.println("ZkSync: Invalid prefix and/or directory argument (NoNode)");
			return false;
		}
		return false;
	}
	
	@Override
	public void process(WatchedEvent event) {
		if (event.getType() == EventType.NodeDeleted && event.getPath().equals(watchPath)) {
			watchPath = null;
			// Wake up the waiting thread
			assert(isMyTurn.getCount() == 1);
			isMyTurn.countDown();
		} else {
			System.err.println("ZkSync: Unhandled watch event: " + event.toString());
		}
	}
}
