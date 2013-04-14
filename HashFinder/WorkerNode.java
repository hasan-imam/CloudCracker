import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.math.BigInteger;

/**
 * Worker node for MD5 hash lookup service
 * Proactively finds jobs to work on (from job pool at the zookeeper)
 * Fetches file partition from file server (gets server's IP from zookeeper)
 * Runs lookup on the fetched part of the dictionary.
 * @author Hasan Imam
 */

public class WorkerNode extends Thread{
	// Connection and keeper info
	/** Host and port info of the zookeeper */
	private String host_port = null;
	/** Zookeeper connector. Used to maintain connection with the zookeeper */
	private ZkConnector zkc = null;
	/** The zookeeper that this client connects to */
	private ZooKeeper zk;
	/** Parent node for the worker nodes */
	private static final String parent_path = "/Workers";
	/** Node path for the primary fileserver */
	private static final String server_path = "/FileServers/Prime";
	/** Node path for the primary fileserver */
	private static final String jobs_path = "/MD5_jobs";
	/** Name of any result node */
	private static final String result_name = "result";
	/** Suffix that differentiates a job partition from its lock node */
	private static final String lock_suffix = "_";
	/** How long the worker should sleep when it can't find a job */
	private static final int snoozeLength = 10000;
	/** Separator worker looks for when parsing for partition number or hash from node path. This MUST be only 1 character long */
	private static final String separator = "_";

	/** This worker's node path at the zookeeper */
	private String my_path = null;
	/** For watching the primary node */
	private Sentinel watcher = new Sentinel();
	/** ID of this worker */
	private String my_ID = null;
	/** For thread status/oprtaion control */
	private boolean running = false;

	/**
	 * Constructor. Sets up internal data, network connections and registers at zookeeper as a worker node
	 * @param host_port <HostIP>:<Port> of the zookeeper 
	 */
	public WorkerNode(String host_port) throws IOException, InterruptedException, KeeperException{
		this.host_port = host_port;

		// Connect with zookeeper
		zkc = new ZkConnector();
		zkc.connect(host_port);
		zk = zkc.getZooKeeper();

		// Register at zookeeper (create node)
		do {
			my_ID = "PROCESSOR_" + String.valueOf(new Random().nextInt(9999)); // Assign a random name
			try {
				my_path = zk.create((parent_path + "/" + my_ID), null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
				assert(my_path != null);
			} catch (KeeperException.NodeExistsException e) {
				// Worker with same name exists. Retry
				my_path = null;
			}
		} while (my_path == null); // my_path only assigned at successful registration
		this.setName(my_ID);
	}

	private void snooze() throws InterruptedException {
		// Nothing to do. Sleep before continuing
		System.out.print(this.getName() + ": going to sleep ... ");
		Thread.sleep(snoozeLength);
		System.out.print("woke up!\n");
	}

	public static int extractPartitionNumber(String path) {
		String temp = path;
		if (path.endsWith(separator)) {
			// Case: It's a lockedPath. Remove the end separator
			temp = path.substring(0, path.length()-1);
		}
		temp = temp.substring(temp.lastIndexOf(separator) + 1); // Extract the last part containing the int
		return Integer.parseInt(temp);
	}
	
	public static String extractHash(String partitionPath) {
		String[] temp = partitionPath.split("/");
		if (temp.length == 4 || temp.length == 3) { 
			// Equals 4 case: it's a job partition path. With correct formation, they break down in 4 parts, the third part containing hash and client_ID
			// Equals 3 case: it's a job node path (parent of partition paths). Has one less split
			temp = temp[2].split(separator);
			assert(temp.length == 2); // Should be exactly 2 parts: Client ID and the hash
			return temp[1];
		} else { // Case: unknown. Invalid argument
			return null;
		}
	}
	
	public static String extractClientID(String jobNodePath) {
		String[] temp = jobNodePath.split("/");
		if (temp.length == 4 || temp.length == 3) { 
			// Equals 4 case: it's a job partition path. With correct formation, they break down in 4 parts, the third part containing hash and client_ID
			// Equals 3 case: it's a job node path (parent of partition paths). Has one less split
			temp = temp[2].split(separator);
			assert(temp.length == 2); // Should be exactly 2 parts: Client ID and the hash
			return temp[0];
		} else { // Case: unknown. Invalid argument
			return null;
		}
	}

	@Override
	public void run(){
		System.out.println(this.getName() + ": operational");
		running = true;
		while (running) {
			try {
				// Find job partitions to work on
				LinkedList<String> jobPartitions = findJob();
				if (jobPartitions == null) {
					snooze();
					continue;
				}

				// Lock on a job on the list
				String lockedJob = getJob(jobPartitions);
				if (lockedJob == null) {
					snooze();
					continue;
				}

				// Fetch dictionary partition from file server
				int partitionNumber = extractPartitionNumber(lockedJob);
				String hash = extractHash(lockedJob);
				assert(hash != null);
				Vector<String> partition = fetchPartition(partitionNumber);

				// Run hash lookup
				System.out.println("JOB: Searching word for hash: " + hash + " in partition # " + partitionNumber);
				String word = lookupHash(partition, hash);
				
				// Commit result (if any)
				if (word != null) {
					System.out.println("JOB: Lookup successful. Word: " + word + ", Hash: " + hash + ". Found in partition # " + partitionNumber);
					try {
						// Found the word for the hash. Create result node.
						String resultPath = lockedJob.substring(0, lockedJob.lastIndexOf('/')) + "/" + result_name;
						String resultPathCreated = zk.create(resultPath, Serializer.serialize(word), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
						assert(resultPathCreated.equals(resultPath));
					} catch (KeeperException.NodeExistsException e) {
						// Some one else already found the result. Unlikely
						System.err.println("Someone already found the result!");
					} catch (KeeperException.NoNodeException e) {
						// Top level job node has been removed. Unlikely
						System.err.println("Job already finished!");
					}
				} else {
					System.out.println("JOB: No word in partition # " + partitionNumber + " has the hash " + hash);
				}
				
				// Clean up locks and job partition from job node
				try {
					zk.delete(lockedJob, -1); // Delete the lock
				} catch (KeeperException.NoNodeException e) {
					System.err.println("Interference with my partition!"); // Acceptable. Means result was already found and tracker dperformed clean up
				}
				try {
					zk.delete(lockedJob.substring(0, (lockedJob.length() - lock_suffix.length())), -1); // Delete the job partition
				} catch (KeeperException.NoNodeException e) {
					System.err.println("Interference with my partition!"); // Acceptable. Means result was already found and tracker dperformed clean up
				}
			} catch (Exception e) {
				running = false;
				System.err.println(this.getName() + ": error encountered.");
				e.printStackTrace();
			} /*catch (InterruptedException e) {
				running = false;
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}*/
		}
		
		System.out.println(this.getName() + "Quitting...");
	}

	private Vector<String> fetchPartition(int partitionNumber) {
		Vector<String> partition = null;
		Socket clientSocket = null;
		ObjectOutputStream out = null;
		ObjectInputStream in = null;
		try {
			// Connecting to server
			clientSocket = getConnection();
			out = new ObjectOutputStream(clientSocket.getOutputStream());
			in = new ObjectInputStream(clientSocket.getInputStream());

			// Fetching
			Packet toServer = new Packet(Packet.PacketType.REQ, partitionNumber, null);
			out.writeObject(toServer);
			Packet fromServer = (Packet) in.readObject();
			if (fromServer.type == Packet.PacketType.REPLY) {
				partition = fromServer.partition;
			} else {
				System.err.println("No partition received from file server.");
				partition = null;
			}

			// Cleanup
			out.close();
			in.close();
			clientSocket.close();
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return partition;
	}

	/**
	 * @return A socket connected to the file server
	 */
	private Socket getConnection() throws NumberFormatException, UnknownHostException, IOException, KeeperException, InterruptedException, ClassNotFoundException {
		String temp = getFileServerInfo();
		if (temp == null) {
			return null;
		}
		String[] FS_IP_Port = temp.split(separator);
		assert(FS_IP_Port.length == 2);
		return new Socket(FS_IP_Port[0], Integer.parseInt(FS_IP_Port[1]));
	}

	/**
	 * @return The current file server's <IP><Separator><port>
	 */
	private String getFileServerInfo() throws KeeperException, InterruptedException, IOException, ClassNotFoundException {
		try {
			Stat stat = new Stat();
			String serverIP = (String) Serializer.deserialize(zk.getData(server_path, false, stat));
			if (serverIP == null) {
				System.err.println("Could not fetch file server's IP");
				return null;
			}
			return serverIP;
		} catch (KeeperException.NoNodeException e) {
			System.err.println("No file server found.");
			return null;
		}
	}

	/**
	 * Find a list of jobs that this worker can set locks on
	 * @return List of job partition absolute paths. If there is no job then returns null 
	 */
	private LinkedList<String> findJob() throws KeeperException, InterruptedException {
		LinkedList<String> hitList = new LinkedList<String>();
		List<String> jobList = zk.getChildren(jobs_path, false); // Get list of current job nodes
		if (jobList != null && jobList.size() > 0) { 
			for (String jobNode : jobList) { // For each job, find partitions that this worker can work on
				// Get list of do-able job paths under this node
				String jobPath = jobs_path + "/" + jobNode;
				List<String> partitionList = prunePartitionList(zk.getChildren(jobPath, false), jobPath + "/");
				if (partitionList != null) {
					// Add to hit list of jobs
					hitList.addAll(partitionList);
				}
			}
		}
		if (hitList.size() > 0) {
			return hitList;
		} else {
			return null;
		}
	}

	/**
	 * Prunes a list of jobs. Looks for already taken jobs and possibility that result has already been found
	 * @param partitionList List of partitions under a certain job node
	 * @param prefix Prefix to add to the beginning of the do-able jobs to get their absolute path
	 * @return A list containing only the do-able jobs converted in absolute paths. Returns null if result is already there, given list is null or no unlocked partition left after pruning
	 */
	private List<String> prunePartitionList(List<String> partitionList, String prefix) {
		if (partitionList == null || partitionList.size() == 0 || partitionList.contains(result_name)) {
			return null;
		}
		List<String> prunedList = new LinkedList<String>();
		for (int i = 0; i < partitionList.size(); i++) {
			String partition = partitionList.get(i);
			if (partition.endsWith(lock_suffix) == false && partitionList.contains(partition + lock_suffix) == false) {
				// This is a partition node && does not have a lock on it. Add to the pruned list
				prunedList.add(prefix + partition);
			}			
		} // Done adding all unlocked partitions to prunedList
		if (prunedList.size() > 0) {
			return prunedList;
		} else {
			return null;
		}
	}

	/**
	 * Sets a lock on one of the jobs in the given list. Takes out the paths that were attempted including the one that is acquired
	 * @return Path on which lock was set. Null if failed to set any lock
	 */
	private String getJob(LinkedList<String> jobPartitions) throws KeeperException, InterruptedException{
		for (Iterator<String> itr = jobPartitions.iterator(); itr.hasNext(); ) {
			String lock_path = null;
			try {
				lock_path = zk.create(itr.next() + lock_suffix, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			} catch (KeeperException.NodeExistsException e) {
				// Lock already exists. Try the next one
				continue;
			} catch (KeeperException.NoNodeException e) {
				// Parent path has been deleted. Possibly done with this job. Try next one
				continue;
			} finally {
				// Remove attempted item from list.
				itr.remove();
			}

			if (lock_path != null) {
				// Locked on a job. Return its path
				return lock_path;
			}
		}
		return null;
	}
	
	public static String lookupHash(Vector<String> partition, String hash){
		for (String word : partition) {
			if (getHash(word).equals(hash)) {
				return word;
			} 
		}
		return null;
	}

	public static String getHash(String word) {
		String hash = null;
		try {
			MessageDigest md5 = MessageDigest.getInstance("MD5");
			BigInteger hashint = new BigInteger(1, md5.digest(word.getBytes()));
			hash = hashint.toString(16);
			while (hash.length() < 32) hash = "0" + hash;
		} catch (NoSuchAlgorithmException nsae) {
			System.err.println("No such algorithm.");
		}
		return hash;
	}

	/**
	 * General watcher class for worker nodes
	 */
	class Sentinel implements Watcher {

		@Override
		public void process(WatchedEvent event) {

		}


	}
}

