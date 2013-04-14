import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import java.awt.*;

/**
 * Framework component that can run on User machines. 
 * Provides a command line user interface for Users to submit Job requests and Job status queries. 
 * Each Job is identified by the password hash. The Client Driver uses ZooKeeper as lookup service
 * for various operations like adding jobs, getting results etc.
 * @author Hasan Imam
 */
public class ClientDriver {
	// Path variables
	/** Path where this client registers it's client node. */
	private static final String client_path = "/Clients";
	/** Path where the primary job tracker lives */
	private static final String prime_path = "/Job_tracker/Prime";
	/** Cache that stores all the hash searches done by this client */
	private static final Map <String, JobInfo> jobCache = new HashMap<String, JobInfo>();

	// Connection and keeper info
	/** Host and port info of the zookeeper */
	private static String host_port = null;

	/** Zookeeper connector. Used to maintain connection with the zookeeper */
	private static ZkConnector zkc = null;

	/** The zookeeper that this client connects to */
	private static ZooKeeper zk;

	// This client's internal data
	private static String client_ID = null;
	private static String my_path = null;
	private static boolean run = false;
	private static BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
	private static boolean primary_isOnline = false; // True if job tracker is known to be online
	private static int seen_version = -1; // Last seen version that was edited by this client
	private static ZkSync syncher = null;

	// Watcher for the client
	private static ClientWatch watcher = new ClientWatch();

	/**
	 * Checks the cache for a previously done job. 
	 * @param hash Hash to look for
	 * @return Plaintext for the hash if it was found. Otherwise null
	 */
	private static String checkCache (String hash) {
		if (jobCache.containsKey(hash)) {
			JobInfo job = jobCache.get(hash);
			assert(job.type == ClientData.JobStat.DONE); // Should only be cached once the processing has been done
			return job.result;
		} else {
			return null;
		}
	}

	/**
	 * Updates local cache with already finished job results. Cleans up the data
	 * @param data Client data to process
	 * @return Number of result cached 
	 */
	private static int cacheIn(ClientData data) {
		int i = 0;
		for(Iterator<Map.Entry<String, JobInfo>> it = data.dataMap.entrySet().iterator(); it.hasNext(); i++) {
			Map.Entry<String, JobInfo> entry = it.next();
			if(entry.getValue().type == ClientData.JobStat.DONE) {
				jobCache.put(entry.getKey(), entry.getValue());
				it.remove();
			}
		}
		return i;
	}

	/**
	 * Submit a new job to the server
	 * @paramr hash Hash string that needs to be looked up
	 * @return True if everything goes successfully. False otherwise
	 */
	private static boolean submitJob (String hash) {
		Stat stat = new Stat();
		boolean gotLock = false;
		try {
			// Get lock on this client's node
			if (syncher.getLock(client_path, client_ID + "-") == false) {
				System.err.println("Could not get lock on my data.");
				return false;
			}
			gotLock = true;
			// Add job info on this client's node data field
			byte[] b = zk.getData(my_path, watcher, stat);
			ClientData data = (ClientData) Serializer.deserialize(b);
			assert(data != null && stat != null); // Data field should never be null
			cacheIn(data); // Update local cache and clean the object

			// Add job to the list
			if (!data.dataMap.containsKey(hash)) {
				data.dataMap.put(hash, new JobInfo(ClientData.JobStat.NEW));
				b = Serializer.serialize(data);
				assert(b != null);
				int version = stat.getVersion();
				stat = zk.setData(my_path, b, version);
				assert(stat != null); 
				seen_version = stat.getVersion();
				return true;
			} else { // Job already exists in the list
				System.out.println("Job: " + hash + " already exists.");
				return false;
			}
		} catch (KeeperException e) {
			e.printStackTrace();
			return false;
		} catch (InterruptedException e) {
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			System.err.println("ERROR: Conversion");
			e.printStackTrace();
			return false;
		} catch (ClassNotFoundException e) {
			System.err.println("ERROR: Conversion");
			e.printStackTrace();
			return false;
		} finally {
			if (gotLock) { // Release the lock
				try {
					syncher.unLock(client_path, client_ID);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * @param hash Hash for the job that needs to be checked
	 * @return Null if job doesn't exist. Empty string ("") if job still in progress. String containing
	 */
	private static ClientData.JobStat jobCheck (String hash) throws KeeperException, InterruptedException, IOException, ClassNotFoundException {
		if (jobCache.containsKey(hash)) {
			return jobCache.get(hash).type;
		}
		Stat stat = null;
		byte[] b = zk.getData(my_path, false, stat);
		if (b == null)
			return null;

		// Status check
		ClientData data = (ClientData) Serializer.deserialize(b);
		return data.getJobType(hash);
	}

	/**
	 * Finds the result of the hash lookup. Should be called once confirmed that processing has finished (i.e. using jobCheck(...))
	 * @param hash Hash whos reply it needs to lookup
	 * @return String with the result if successful. Otherwise null
	 */
	private static String getResult (String hash) {
		if (jobCache.containsKey(hash) && jobCache.get(hash).type == ClientData.JobStat.DONE) {
			return jobCache.get(hash).result;
		}
		
		Stat stat = new Stat();
		try {
			byte[] b = zk.getData(my_path, watcher, stat);
			assert(b != null);
			ClientData data = (ClientData) Serializer.deserialize(b);
			jobCache.put(hash, data.getJobInfo(hash));
			return data.getJobResult(hash);
		} catch (Exception e) {
			e.printStackTrace();
		}/* catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}*/
		return null;
	}

	/**
	 * Registers with the zookeeper by creating the client node
	 * Exits the system if the service is not online
	 * @return true if the process was successful
	 */
	private static boolean init(String uname) {
		try {
			// Create the client node (/Clients/<my_client_ID>) at the zookeeper (Registration)
			my_path = zk.create(
					(client_path + "/" + uname),           // Path of this client's znode
					Serializer.serialize(new ClientData()),// Initial data field. Empty 
					Ids.OPEN_ACL_UNSAFE,    // ACL, set to Completely Open.
					CreateMode.EPHEMERAL);  // Znode type, set to Ephemeral.	
		} catch (IllegalStateException e) {
			System.out.println("Zookeeper get - "+ e.getMessage());
			return false;
		} catch(KeeperException e) {
			System.out.println("Zookeeper create - " + e.code());
			return false;
		} catch(Exception e) {
			System.out.println("Zookeeper connect - "+ e.getMessage());
			return false;
		}

		if (my_path != null) {
			System.out.println("Registered as a client.");	
			client_ID = uname;
			return true;
		}
		return false;
	}

	/**
	 * Initialize connection with the zookeeper.
	 * Prompt user for username.
	 * Register user with given username.
	 */
	private static boolean register() {		
		try {
			// Create the connector and connect with at the given host/port address
			zkc = new ZkConnector();
			zkc.connect(host_port);
			zk = zkc.getZooKeeper(); // Get the initialized zookeeper
			System.out.println("Zookeeper found.");

			// Add watch to the current primary tracker 
			Stat stat = zk.exists(prime_path, watcher);
			if (stat == null) {
				System.out.println("Service is offline! Please try again later.");
				zk.close();
				System.exit(1);
				return false;
			} else {
				primary_isOnline = true;
				System.out.println("Job tracker found.");
			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		// Take in username/client_ID and try to register
		run = false;
		while(!run) {
			System.out.print("Enter username: ");
			try {
				String uname = br.readLine();
				if (init(uname)) {
					// Registration successful
					run = true;
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return true;
	}

	/**
	 * @param args ZookeeperIP:port
	 */
	public static void main (String args[]) throws IOException {
		if (args.length != 1) {
			System.out.println("Error: Need to provide <zkServer:clientPort>");
			return;
		}
		host_port = args[0];

		// Welcome screen
		System.out.print("Welcome to NutCracker!\n");

		// Register
		register();
		syncher = new ZkSync(zk); // Create sychronization helper

		String command = null;
		while (run) {
			try {				
				// Read command
				System.out.print(">> ");
				command = br.readLine();
				if (command == null) {
					continue;
				}
				String[] comms = command.split(" ");
				if (comms.length == 0 || comms.length > 2) 
					continue;

				// Process commands
				if (comms[0].equalsIgnoreCase("job")) { // Case: Add new job
					if (comms.length == 2) {
						if (jobCache.containsKey(comms[1])) { // See if cache already has the result
							JobInfo job = jobCache.get(comms[1]);
							assert(job.type == ClientData.JobStat.DONE); // Should only be cached once the processing has been done
							if (job.result != null) {
								System.out.println("Hash: " + comms[1] + " | Password: " + job.result + ". [Cache]");
							} else {
								System.out.println("Plain text for this hash was not found. [Cache]");
							}
						} else if (submitJob(comms[1])) {
							System.out.println("Job added successfully.");
							continue;
						} else {
							System.err.println("Failed to add job.");
							continue;
						}
					} else {
						System.err.println("Invalid command.");
						continue;
					}
				} else if (comms[0].equalsIgnoreCase("query")) { // Case: Query an existing job
					if (comms.length == 2) {
						ClientData.JobStat type = jobCheck(comms[1]);

						if (type == null) { // Job doesnt exist
							System.out.println("No such job exists.");
						} else if (type == ClientData.JobStat.NEW) {
							System.out.println("Waiting to be processed");
						} else if (type == ClientData.JobStat.PROCESSING) {
							System.out.println("Being processed");
						} else if (type == ClientData.JobStat.DONE) {
							String result = getResult(comms[1]);
							if (result != null) {
								System.out.println("Hash: " + comms[1] + " | Password: " + result);
							} else {
								System.out.println("Plain text for this hash was not found.");
							}
						}
					} else {
						System.err.println("Invalid command.");
						continue;
					}
				} else if (comms[0].equalsIgnoreCase("quit")) { // Case: Quit client driver
					System.out.println("Quitting...");
					run = false;
					zkc.close();
					continue;
				} else if (comms[0].equalsIgnoreCase("rm")) { // Case: Remove a job
					System.out.println("Not implemented.");
					/*if (comms.length == 2) {
						// delete the job
						System.out.println("Job deleted: " + comms[1]);
					} else {
						System.err.println("Invalid command.");
						continue;
					}*/
				} else { // Case: Invalid command
					System.out.println("Unknown command: " + comms[0]);
				}
			} catch (IOException ioe) {
				System.err.println("IO error trying to read command!");
				System.exit(1);
			} catch (InterruptedException e) {
				System.err.println("Error while closing the zk connector.");
				e.printStackTrace();
			} catch (KeeperException e) {
				System.err.println("Error - keeper exception.");
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				System.err.println("Error - keeper exception.");
				e.printStackTrace();
			}
		} // End of while (run loop)
	}

	static class ClientWatch implements Watcher{
		@Override
		public void process(WatchedEvent event) {
			try {
				// Case: Disconnected from zookeeper
				if (event.getState() == KeeperState.Disconnected) {
					System.out.println("Disconnected from zookeeper. Quitting...");
					run = false;
					try {
						zkc.close();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				} 
				// Case: A watched node was deleted
				else if (event.getType().equals(EventType.NodeDeleted)) {
					handle_nodeDeletion(event);
				} 
				// Case: A watched node has been created
				else if (event.getType().equals(EventType.NodeCreated)) {
					handle_nodeCreation(event);
				} 
				// Case: Node data change
				else if (event.getType().equals(EventType.NodeDataChanged)) {
					handle_nodeDataChange(event);
				} 
				else {
					System.out.println("Unexpected watch event: " + event.toString());
				}
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		private void handle_nodeDataChange(WatchedEvent event){
			// Case: Event on this client's node
			if (event.getPath().equals(my_path)) {
				// Current implementation does not need us to do anything with this case. Just play an alarm.
				Toolkit.getDefaultToolkit().beep();
			}
			else {
				System.out.println("Ignored node change: " + event.getPath());
			}
		}

		private void handle_nodeDeletion(WatchedEvent event) throws KeeperException, InterruptedException {
			// Case: Primary tracker died
			if (event.getPath().equals(prime_path)) {
				primary_isOnline = false;
				// Watch out for new primary creation
				Stat stat = zk.exists(prime_path, this);
				if (stat != null) {
					// Prime has come online by the time we are processing this event
					primary_isOnline = true;
				}
			} 
			else {
				System.out.println("Ignored node deletion: " + event.getPath());
			}
		}

		private void handle_nodeCreation(WatchedEvent event) throws KeeperException, InterruptedException {
			// Case: new prime online
			if (event.getPath().equals(prime_path)) {
				primary_isOnline = true;
				// Put new watch
				Stat stat = zk.exists(prime_path, this);
				assert(stat != null); // Prime should be online at this point
			}
			else {
				System.out.println("Ignored node creation: " + event.getPath());
			}
		}
	}
}

