import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

/**
 * Job tracker that connects the bridge between incoming user jobs and the worker pool.
 * It proactively looks for clients submitting jobs. When it finds a new job, it takes 
 * the necessary steps needed to allow the worker pool to get cracking.
 * 
 * If more than one instances are being run (doesn't matter on which node) only one
 * of them works as the primary, all others being backup. If primary fails the new 
 * primary takes over from where the previous one left off
 * 
 * @author Hasan Imam
 */
public class JobTracker extends Thread{
	// Path variables
	/** Path where the job trackers register. */
	private static final String tracker_path = "/Job_tracker";
	/** Path where the clients register */
	private static final String client_path = "/Clients";
	/** Path where the workers register */
	private static final String worker_path = "/Workers";
	/** Path where the jobs are managed */
	private static final String job_path = "/MD5_jobs";
	/** Parent node for the fileservers */
	private static String FS_path = "/FileServers";
	/** Name of the primary */
	private static final String primary = "Prime";
	/** Number of partitions any MD5 job will be divided into */
	private static final int partitions = 6;
	/** Name of any result node */
	private static final String result_name = "result";
	/** Separator worker looks for when parsing for partition number or hash from node path. This MUST be only 1 character long */
	private static final String separator = "_";

	// Connection and keeper info
	/** Host and port info of the zookeeper */
	private final String host_port;

	/** Zookeeper connector. Used to maintain connection with the zookeeper */
	private ZkConnector zkc = null;

	/** The zookeeper that this client connects to */
	private ZooKeeper zk;

	// Internal data
	/** Node name at the zookeeper that corresponds to this tracker.
	 * Random id used when regsitering as backup. If primary job tracker then called {@link primary} */
	private String tracker_id = null;

	/** Node path for this tracker at the zookeeper */
	private static String my_path = null;

	/** General watcher for the tracker */
	private TrackerWatch watcher = new TrackerWatch();
	private PrimeWatch primeWatcher = new PrimeWatch();
	private ZkSync syncher = null;

	/** A synchronization aid for signalling when this tracker becomes the prime tracker. */
	private CountDownLatch becamePrimeSignal;
	/** A synchronization aid for signalling when this tracker gets disconnected from zookeeper. */
	public CountDownLatch isDisconnected = new CountDownLatch(1);

	public JobTracker(String host_port) {
		super("JobTracker");
		assert(host_port != null);
		this.host_port = host_port;
	}

	/**
	 * Initialize (if needed) the paths needed to be in the zookeeper to initialize the service.
	 * @return True is initialization was successful. False if there is something wrong with the path hierarchy at the zookeeper
	 */
	private boolean initService() {
		assert(zkc != null && zk != null);
		System.out.println("Initializing services...");
		try {
			// Try to create parent nodes. If they exist, then will get exception
			// Tracker parent
			try {
				// Create initialization data. Watch list should contain all the paths a tracker needs to watch at all times
				JobTrackerData data = new JobTrackerData();
				data.watchList.add(client_path); // Need to know if a client disappreas, adds new job or new client joins etc.
				data.watchList.add(job_path); // Need to know what's happening to the jobs in the pool 
				data.watchList.add(worker_path); // Just to know when a worker joins
				data.watchList.add(tracker_path); // Not critically needed right now, but keep watching for status updates and error checks
				// File server not added in the list
				
				String result = zk.create(tracker_path, Serializer.serialize(data), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT); 
				System.out.println("Initialized job tracker service @ " + result);
			} catch (KeeperException.NodeExistsException e) {
				System.out.println("Job tracker service already online.");
			}

			// FileServer parent
			try {
				String result = zk.create(FS_path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT); 
				System.out.println("Initialized file server operations @ " + result);
			} catch (KeeperException.NodeExistsException e) {
				System.out.println("File server operations already online.");
			}

			// Worker parent
			try {
				String result = zk.create(worker_path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT); 
				System.out.println("Initialized worker service @ " + result);
			} catch (KeeperException.NodeExistsException e) {
				System.out.println("Worker service already online.");
			}

			// Job parent folder
			try {
				String result = zk.create(job_path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT); 
				System.out.println("Initialized job management service @ " + result);
			} catch (KeeperException.NodeExistsException e) {
				System.out.println("Job management service already online.");
			}

			// Client parent
			try {
				String result = zk.create(client_path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT); 
				System.out.println("Initialized client service @ " + result);
			} catch (KeeperException.NodeExistsException e) {
				System.out.println("Client service already online.");
			}

			return true;
		} catch (KeeperException e) {
			e.printStackTrace();			
			return false;
		} catch (InterruptedException e) {
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}

	private boolean register() {
		try {
			// Connect with the keeper
			zkc = new ZkConnector();
			zkc.connect(host_port);
			// Get the initialized zookeeper
			zk = zkc.getZooKeeper();
			System.out.println("Zookeeper found.");
			// Check whether services (parent nodes) is already set up or not. Setup if needed
			initService();
			// Initialize synchronizer
			syncher = new ZkSync(zk);

			// Try to register as primary. If failed then first register as backup then keep waiting till it becomes primary
			if (registerAsPrime()) {
				System.out.println("Registered as primary.");
				return true;
			} else if (registerAsBackup()) {
				// Successfully registered as backup
				System.out.println("Registered as backup: " + tracker_id);
				// Set watch on current primary
				Stat stat = zk.exists(tracker_path + "/" + primary, primeWatcher);
				assert(stat != null);

				// Wait/block until it becomes the primary. See PrimeWatch
				becamePrimeSignal = new CountDownLatch(1);
				becamePrimeSignal.await();
				assert(tracker_id.equals(primary)); // Should be the primary when unblocks
				return true;
			} else {
				System.out.println("Failed to register as either primary or backup.");
				return false;
			}
		} catch (IllegalStateException e) {
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		} catch (InterruptedException e) {
			e.printStackTrace();
			return false;
		} catch (KeeperException e) {
			e.printStackTrace();
			return false;
		}
	}

	private boolean registerAsBackup() {
		boolean retry = false;
		do {
			// Attempt to become a backup
			try {
				tracker_id = "B_" + String.valueOf(new Random().nextInt(9999)); // Random 4-digit id
				my_path = zk.create(
						(tracker_path + "/" + tracker_id),   // Path of this tracker's znode
						null,								// Initial data field. Empty 
						Ids.OPEN_ACL_UNSAFE,    // ACL, set to Completely Open.
						CreateMode.EPHEMERAL);  // Znode type, set to Ephemeral.
			} catch (KeeperException e) {
				if (e instanceof KeeperException.NodeExistsException) {
					System.out.println("Backup " + tracker_id + " already exists. Retrying...");
					retry = true;
				} else {
					e.printStackTrace();
					return false;
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
				return false;
			}
		} while (retry); // Retry only when another backup with the same name exists. Not for any other error

		return true;
	}

	/**
	 * Attempts to register this tracker as the primary.
	 * @return True if successful.
	 */
	private boolean registerAsPrime() {
		// Attempt to become prime
		try {
			my_path = zk.create(
					(tracker_path + "/" + primary),      // Path of this tracker's znode
					null,								// Initial data field. Empty 
					Ids.OPEN_ACL_UNSAFE,    // ACL, set to Completely Open.
					CreateMode.EPHEMERAL);  // Znode type, set to Ephemeral.
		} catch (KeeperException e) {
			if (e instanceof KeeperException.NodeExistsException) {
				System.out.println("Primary already exists.");
				return false;
			} else {
				e.printStackTrace();
				return false;
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
			return false;
		}

		// Registered as primary
		tracker_id = primary;
		return true;
	}

	/**
	 * Performs setups needed to start tracking.
	 * Sets watch on whatever the previous tracker was watching (if there were any previous tracker)
	 * @return True if operations were successful. Throws exceptions if error occurs
	 */
	private boolean recovery() throws KeeperException, InterruptedException, IOException, ClassNotFoundException {
		// Fetch tracker data
		Stat stat = new Stat();
		byte[] b = zk.getData(tracker_path, false, stat);
		assert(b != null && stat != null);
		JobTrackerData data = (JobTrackerData) Serializer.deserialize(b);

		// Adding watch on paths on the watch list
		System.out.println("Watching: ");
		for(String watch_path : data.watchList){
			stat = zk.exists(watch_path, watcher);
			if (stat != null) {
				System.out.println("\t" + watch_path);
				// Add watch on children
				List<String> children = zk.getChildren(watch_path, watcher, stat);
				if (children.size() > 0) {
					for (String child : children) {
						String childPath = watch_path + "/" + child;
						stat = zk.exists(childPath, watcher);
						if (stat != null) {
							System.out.println("\t" + childPath);
						}
					}
				}
			} else {
				System.err.println("\t" + watch_path + " (not found)");
			}
		}

		return true;
	}

	/**
	 * Tracker thread
	 */
	@Override
	public void run() {
		try {
			
			// Register. Initialize service if needed
			register();

			// Run recovery/setup
			recovery();

			// Wait until getting disconnected from zookeeper.
			isDisconnected.await();
			System.out.println("Disconnected from zookeeper. Quitting...");
			zkc.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Internal watcher to check when the primary node gets disconnected.
	 * Retries to become the primary when gets a chance
	 */
	class PrimeWatch implements Watcher{
		@Override
		public void process(WatchedEvent event) {
			// Check for event type NodeCreated
			boolean isNodeDeleted = event.getType().equals(EventType.NodeDeleted);
			// Verify if this is actually the prime znode
			boolean isPrime = event.getPath().equals(tracker_path + "/" + primary);
			if (isNodeDeleted && isPrime) {
				assert(my_path.equals(tracker_path + "/" + primary) == false); // This cannot be the primary at this point
				System.out.println("Previous prime has died! Attempting to become the prime...");
				if (registerAsPrime()) {
					System.out.println("Became the primary.");
					assert(becamePrimeSignal.getCount() == 1);
					becamePrimeSignal.countDown(); // Need to release the main thread now. Assuming initialized to 1
				} else {
					System.out.println("Didn't become the prime. Will try again later.");
					// Set watch on the new primary
					try {
						Stat stat = zk.exists(tracker_path + "/" + primary, this);
						assert(stat != null);
					} catch (KeeperException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			} else {
				System.out.println("PrimeWatch: Unexpected watch event: " + event.toString());
			}
		}
	}

	/**
	 * General watcher class for job trackers
	 */
	class TrackerWatch implements Watcher{

		@Override
		public void process(WatchedEvent event) {
			System.out.println("WATCHER: processing event: " + event.toString());
			try {
				if (event.getState() == KeeperState.Disconnected) {
					System.out.println("Got disconnected from zookeeper.");
					isDisconnected.countDown();
				} else if (event.getType().equals(EventType.NodeDataChanged)) {
					handler_nodeDataChange(event);
				} else if (event.getType().equals(EventType.NodeChildrenChanged)) {
					handler_nodeChildrenChange(event);
				} else if (event.getType().equals(EventType.NodeDeleted)) {
					handler_nodeDeleted(event);
				} else if (event.getType().equals(EventType.NodeCreated)) {
					handler_nodeCreation(event);
				} else {
					System.out.println("unhandled event: " + event.toString());
				}
			} catch (Exception e) {
				System.err.println("Processing exception. Message: " + e.getMessage());
				e.printStackTrace();
			}
		}

		private void handler_nodeCreation(WatchedEvent event) throws KeeperException, InterruptedException, IOException, ClassNotFoundException {
			// Case: Result node for some job has been created.
			if (event.getPath().endsWith(result_name)) {
				// Fetch result
				Stat stat = new Stat();
				String word = (String) Serializer.deserialize(zk.getData(event.getPath(), this, stat));
				// Update the client's node
				updateResult(word, event.getPath());
				// Remove from tracker watch list
				JobTrackerData data = (JobTrackerData) Serializer.deserialize(zk.getData(tracker_path, this, stat));
				boolean result = data.watchList.remove(event.getPath());
				assert(result);
				stat = zk.setData(tracker_path, Serializer.serialize(data), stat.getVersion());
				assert(stat != null);
			} else {
				System.out.println("unhandled nodeCreation event: " + event.toString());
			}
		}

		private void handler_nodeDeleted(WatchedEvent event) throws KeeperException, InterruptedException, IOException, ClassNotFoundException {
			// Case: A client that was being watched was deleted/died
			if (event.getPath().startsWith(client_path)) {
				String client_ID = event.getPath().substring(client_path.length() + 1); // Extract the name of the client. +1 to get rid of the preceding / in its directory path
				// Check if the client had any on going job
				List<String> children = zk.getChildren(job_path, this);
				// Prune list to contain only the jobs created by this deleted client
				for (int i = 0; i < children.size(); ) {
					String child = children.get(i);
					if (child.startsWith(client_ID + separator) == false) {
						// Not a job of the deleted node. Remove from list
						children.remove(child);
					} else {
						// This is a job of the deleted client. Keep on the list
						i++;
					}
				} // After this children list only contains the jobs of the deleted node
				
				// yakety yak
				System.out.println("Client " + client_ID + " died.");
				if (children.size() == 0) {
					return;
				}
				System.out.println("Client " + client_ID + " died with " + children.size() + " ongoing job(s). Stat: " + children.toString());
				System.out.println("Performing cleanup...");
				
				// Delete all the jobs on this list
				for (String jobNodePath : children) {
					try {
						cleanupJob(job_path + "/" + jobNodePath);
					} catch (Exception e) {
						System.out.println("Clould not clean job node: " + job_path + "/" + jobNodePath);
						e.printStackTrace();
					}
				}
			} 
			else {
				System.out.println("Unhandled nodeDeletion event: " + event.toString());
			}
		}

		/**
		 * Updates the given plain text as the result related to the job at given job path or result path
		 * Also performs clean up on the job node.
		 * @param jobNodePath Either the path of the job node or the path of job node's result node
		 * @param word Plaintext found as the result of lookup
		 * @return True if the updating operation was successful
		 */
		private boolean updateResult(String word, String jobNodePath) throws IOException, ClassNotFoundException, KeeperException, InterruptedException {
			boolean status = false;
			Stat stat = new Stat();
			// Update the client's node with the given result
			String hash = WorkerNode.extractHash(jobNodePath);
			String clientID = WorkerNode.extractClientID(jobNodePath);
			assert(hash != null && clientID != null);
			String clientNodePath = client_path + "/" + clientID; // Create path to client's node
			ClientData data;
			try {
				data = (ClientData) Serializer.deserialize(zk.getData(clientNodePath, this, stat));
			} catch (KeeperException.NoNodeException ex) {
				// Client node does not exist. Means client died with jobs running. Get rid of the jobs
				cleanupJob(jobNodePath);
				System.out.println(clientID + " disconnected with job: " + hash + "running. Cleaned up job path.");
				return false;
			}
			JobInfo jobinfo = data.dataMap.get(hash);
			assert(jobinfo != null);
			if (jobinfo.type == ClientData.JobStat.DONE) { // Result already there
				System.out.println("Result has already been updated.");
				cleanupJob(jobNodePath);
				status = false;
			} else { // Store result on client node
				jobinfo.result = word; 
				jobinfo.type = ClientData.JobStat.DONE;
				jobinfo = data.dataMap.put(hash, jobinfo);
				assert(jobinfo != null);
				try {
					syncher.getLock(client_path, clientID);
					stat = zk.setData(clientNodePath, Serializer.serialize(data), stat.getVersion());
					syncher.unLock(client_path, clientID);
					status = true;
				} catch (KeeperException.NoNodeException e) {
					// Client left!
					System.out.println("Client left while updating result for one of it's jobs."); // Not an error case
					status = false;
				} catch (KeeperException.BadVersionException e) {
					// Version mismatch. Means client might have put new data while we were updating here. TODO Retry
					System.out.println("Client node updated while committing result.");
					status = false;
				}
			}
			
			if (status) {
				System.out.println("Updated result for client: " + clientID + ". Hash: " + hash + ", plain text: " + word);
			} else {
				System.out.println("Could not updated result for client: " + clientID + ". Hash: " + hash + ", plain text: " + word);
			}
			cleanupJob(jobNodePath);
			return status;
		}
		
		/**
		 * Cleans up the job node associated with the given path
		 * Removes this path from the watch list of the tracker
		 * @param jobNodePath Path to the job node or path the result node inside it
		 */
		private void cleanupJob(String jobNodePath) throws KeeperException, InterruptedException, IOException, ClassNotFoundException {
			if (jobNodePath.endsWith(result_name)) {
				jobNodePath = jobNodePath.substring(0, jobNodePath.lastIndexOf(result_name) );
			}
			System.out.println("Cleaning up job node: " + jobNodePath);
			// Delete all children
			List<String> children = zk.getChildren(jobNodePath, this);
			for (String child : children) {
				try {
					zk.delete(jobNodePath + "/" + child, -1);
				} catch (KeeperException.NoNodeException e) {
					// Means this node has been deleted by some other entity. Don't care
				}
			}
			// Delete job node
			zk.delete(jobNodePath, -1);
			// Remove from watch list
			Stat stat = new Stat();
			JobTrackerData data = (JobTrackerData) Serializer.deserialize(zk.getData(tracker_path, this, stat));
			if (data.watchList.remove(jobNodePath)) {
				// Deletion successful. Update data
				stat = zk.setData(tracker_path, Serializer.serialize(data), -1); // Version number does not matter since only tracker writes on this node. No chance of collision
				assert(stat != null);
			}
		}
		
		/**
		 * Given a job node, it checks if all partition nodes inside it are done (deleted)
		 * @param jobNodePath Job node to look into. Will get wrong result if it's not a job node path
		 * @return True if all job partitions and their locks have been removed, but no result was found
		 */
		private boolean isAllPartitionDone(String jobNodePath) throws KeeperException, InterruptedException {
			// Check childrens
			List<String> children = zk.getChildren(jobNodePath, this); // Extract children and reset watch
			if (children.size() == 0) {
				System.out.println("All partitions done @ " + jobNodePath);
				return true;
			} else if (children.size() == 0 && children.contains(result_name)) {
				System.out.println("All partitions done @ " + jobNodePath + ". And result node created (plaintext found).");
				return true;
			} else if (children.contains(result_name)) {
				System.out.println("Result node created (plaintext found). Didn't have to process all the partitions.");
				return false;
			} else {
				System.out.println(children.size() + " partitions/other nodes left @ " + jobNodePath);
				return false;
			}
		}

		/** 
		 * @param path Job node partition to check
		 * @return If the given path is a path to a job partition node or its lock, returns the parsed job node. Null otherwise
		 */
		private String checkJobPartitionNode (String path) {
			// Check for some node path characteristics
			String[] dir = path.split("/"); // A job node will split into 4 patritions
			if (dir.length == 4 && dir[3].startsWith("P_")) {
				// Now create the job node path
				String jobNodePath = "/" + dir[1] + "/" + dir[2];
				return jobNodePath;
			}
			return null;
		}

		private void handler_nodeChildrenChange(WatchedEvent event) throws KeeperException, InterruptedException, IOException, ClassNotFoundException {
			// Re-watch all the children
			List<String> children = zk.getChildren(event.getPath(), this); // resets watch and gets a list
			// Case: New client has been added
			if (event.getPath().equals(client_path)) {
				for(String child_path : children){
					child_path = client_path + "/" + child_path;
					Stat stat = zk.exists(child_path, this);
					assert(stat != null);
					System.out.println("Watching: " + child_path);
				} // This might redundantly add watches but it's acceptable
			} 
			// Case: A specific job node's child got deleted. Event received on the job node itself, not the partition
			else if (event.getPath().startsWith(job_path) && (event.getPath().split("/").length == 3)) { // Note: this type of path always splits in 3 parts
				// Check if all the partitions are processed or not 
				if (isAllPartitionDone(event.getPath())) {
					// All partitions are done being processed but nothing was found. Need to update the client node.
					updateResult(null, event.getPath());
				}
			}
			// Case: New worker has joined
			else if (event.getPath().equals(worker_path)) {
				System.out.println("Worker pool changed.");
			} 
			// Case: New job has been added
			else if (event.getPath().equals(job_path)) {
				System.out.println("Job pool changed.");
			} 
			else {
				System.out.println("unhandled childrenchange event: " + event.toString());
			}
		}

		private void handler_nodeDataChange(WatchedEvent event) throws KeeperException, InterruptedException, IOException, ClassNotFoundException {
			// Case: Client updated it's node data
			if (event.getPath().startsWith(client_path)) {
				boolean needUpdate = false; // Whether the node needs to be updated or not
				Stat stat = new Stat();
				ClientData data = (ClientData) Serializer.deserialize(zk.getData(event.getPath(), this, stat)); // Reset watch and fetch data
				String client_ID = event.getPath().substring(client_path.length());
				for (Map.Entry<String, JobInfo> entry : data.dataMap.entrySet()) {
					// Find jobs that are new
					if (entry.getValue().type == ClientData.JobStat.NEW) {
						// Add to job pool if NEW job
						if (addJob(entry.getKey(), client_ID)) {
							System.out.println("Added new job. Hash: " + entry.getKey());
							needUpdate = true;
							entry.getValue().type = ClientData.JobStat.PROCESSING;
							data.dataMap.put(entry.getKey(), entry.getValue()); // Data map being updated inside
						} else {
							System.out.println("Could not add new job. Hash: " + entry.getKey());
						}
					}
				} // Done adding jobs
				if (needUpdate) {
					// Need to update client node
					System.out.println("Updated job for client: " + client_ID);
					boolean gotLock = false;
					try {
						if (syncher.getLock(client_path, client_ID.substring(1))) {
							gotLock = true;
							stat = zk.setData(event.getPath(), Serializer.serialize(data), stat.getVersion());
						} else {
							System.err.println("ERROR: could not acquire lock!");
							assert(true);
						}
					} catch (KeeperException.NoNodeException e) {
						// Rare case of client exiting in the middle of adding all these operations for it.
						// No need to do anything since the client watch event handling will take care of it
						System.err.println("CORNER: Client disappeared while tracker was adding its job");
					} finally { // unlocking
						if (gotLock && syncher.unLock(client_path, client_ID.substring(1)) == false) {
							System.err.println("ERROR: could not unlock an acquired lock!");
							assert(true);
						} 
					}
				}
			} 
			// Case: Unhandled (should not occur)
			else {
				Stat stat =zk.exists(event.getPath(), this);
				assert(stat != null);
				System.out.println("unhandled watch on path: " + event.getPath());
			}
		}

		private boolean addJob(String hash, String client_ID) throws InterruptedException, KeeperException, IOException, ClassNotFoundException {
			// Add to top level job pool
			String thisJobPath = job_path + client_ID + separator + hash;
			LinkedList<Op> ops_list = new LinkedList<Op>();
			// Add main node for this job
			ops_list.add(Op.create(thisJobPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)); 
			// Add the partition nodes
			for (int i = 1; i <= partitions; i++) {
				String partition_path = thisJobPath + "/P_" + Integer.toString(i);
				ops_list.addLast(Op.create(partition_path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)); 
			}
			// Commit operation
			List<OpResult> results = zk.multi(ops_list);
			System.out.println(results.toString());//RM

			// Add watch on the result node and on this job path
			String resultPath = thisJobPath + "/" + result_name;
			Stat stat = zk.exists(resultPath, this);
			assert(stat == null); // Result coiuld not possilby be there already
			// Add to tracker node's watch list. Used for recovery in case of failure
			JobTrackerData data = (JobTrackerData) Serializer.deserialize(zk.getData(tracker_path, this, stat));
			data.watchList.add(resultPath);
			stat = zk.setData(tracker_path, Serializer.serialize(data), -1); // Version number does not matter since only tracker writes on this node. No chance of collision
			assert(stat != null);
			List<String> temp = zk.getChildren(thisJobPath, this); // Set watch on this job node
			assert(temp != null && temp.size() > 0);
			System.out.println("Watching: " + thisJobPath + " & " + resultPath);
			return true;
		}
	}
}

class JobTrackerData implements Serializable {
	private static final long serialVersionUID = 3701631526684472662L;

	/** List of paths that needs to be watched by the tracker. To be used during recovery */
	public LinkedList <String> watchList = new LinkedList<String>();
}
