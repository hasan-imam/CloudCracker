import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

/**
 * Simple file server that serves file partitions to the hash lookup workers
 * @author mdhasanimam
 */
public class FileServer {

	// Connection and keeper info
	/** Host and port info of the zookeeper */
	private static String host_port = null;
	/** Port where this server listens to */
	private static int my_port = -1;
	/** Zookeeper connector. Used to maintain connection with the zookeeper */
	private static ZkConnector zkc = null;
	/** The zookeeper that this client connects to */
	private static ZooKeeper zk;
	/** Parent node for the fileservers */
	private static String parent_path = "/FileServers";
	/** Name of the primary */
	private static final String primary = "Prime";

	/** Path to dictionary */
	private static String filepath = null;
	/** Number of partitions to divide the file into */
	private static final int partitions = 6;
	/** Number of strings/keys in each partition */
	private static final int keyCount = 51200;
	/** 2D container of keys/strings in dictionary. Contains {@link partitions} number of vectors of keys each with size {@link keyCount} or less */
	private Vector keys = null;
	/** Node path for this server at the zookeeper */
	private static String my_path = null;
	/** A synchronization aid for signalling when this server becomes the prime server. */
	private static CountDownLatch becamePrimeSignal = new CountDownLatch(1);
	/** For watching the primary node */
	private static PrimeWatch primeWatcher = new PrimeWatch();
	/** This server's IP address */
	private static String myIP = null;
	/** Separator used to separate host IP and port information. This MUST be only 1 character long */
	private static final String separator = "_";

	/**
	 * Attempts to register this server as the primary.
	 * If fails then adds a watch on the current primary.
	 * To be used within the watch handler and the main thread
	 * @return True if successful.
	 */
	private static boolean registerAsPrime() {
		// Attempt to become prime
		try {
			my_path = zk.create((parent_path + "/" + primary), Serializer.serialize(myIP + separator + my_port), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			assert(my_path != null);
			System.out.println("Became the primary.");
			becamePrimeSignal.countDown();
		} catch (KeeperException.NodeExistsException e) {
			// Some other node has already bacome prime. Set watch
			System.out.println("Primary already exists.");
			Stat stat;
			try {
				stat = zk.exists(parent_path + "/" + primary, primeWatcher);
				assert(stat != null);//RM
			} catch (Exception e1) {
				e1.printStackTrace();
				return false;
			}
			assert(stat != null);
			return false;
		} catch (Exception e) {
			my_path = null;
			e.printStackTrace();
			return false;
		}
		// Registered as primary
		return true;
	}
	
	/**
	 * @param args ZookeeperIP:port my_port dictionary_path my_IP
	 */
	public static void main(String[] args) {
		if (args.length != 4) {
			System.out.println("Error: Bad arguments. Expected: <zkServerIP>:<zkPort> <my listening port> <dictionary_path>");
			return;
		}
		host_port = args[0];
		my_port = Integer.parseInt(args[1]);
		filepath = args[2];
		myIP = args[3];
		
		Scanner scanner = null;
		try {
			// Initialize the dictionary matrix of keys by reading the file
			System.out.print("Loading the dictionary ... ");
			scanner = new Scanner(new FileInputStream(filepath));
			Vector keys = new Vector(partitions);
			for(int i = 0; i < partitions; i++) {
				Vector<String> chapter = new Vector<String>(keyCount);
				for(int j = 0; j < keyCount && scanner.hasNextLine(); j++) {
					chapter.add(scanner.nextLine());
				}
				keys.add(i, chapter);
			}
			System.out.println("done.");
			
			// Connect with zookeeper
			System.out.println("Connecting with zookeeper...");
			zkc = new ZkConnector();
			zkc.connect(host_port);
			zk = zkc.getZooKeeper();
			System.out.println("Connected with zookeeper.");
			
			// Open listening port
			ServerSocket serverSocket = null;
			serverSocket = new ServerSocket(my_port);
			//myIP = InetAddress.getLocalHost().getHostAddress(); //serverSocket.getInetAddress().getHostAddress();
			System.out.println("My IP: " + myIP + ". Listening port: " + my_port);
			
			// Register as a server (by creating primary node)
			if (registerAsPrime() == false) {
				// Prime already exists. Keep an watch and try again through the event handler
				becamePrimeSignal.await(); // Counted down through event handler and thus unblocks the main thread
			} // After unblocking this node should be the prime
			assert(my_path != null);
			
			boolean running = true;
			do {
				ObjectOutputStream out = null;
				ObjectInputStream in = null;
				Socket socket = null;
				try {
					// Listen
					socket = serverSocket.accept();
					out = new ObjectOutputStream(socket.getOutputStream());
					in = new ObjectInputStream(socket.getInputStream());
					// Handle one request
					Packet rcvd = (Packet) in.readObject();
					if (rcvd.type == Packet.PacketType.REQ && rcvd.partitionNumber <= partitions && rcvd.partitionNumber > 0) {
						// Send partition in packet
						Packet reply = new Packet(Packet.PacketType.REPLY, rcvd.partitionNumber, ((Vector<String>) keys.get(rcvd.partitionNumber - 1)));
						out.writeObject(reply);
						System.out.println("Served partition # " + rcvd.partitionNumber);
					} else {
						// Garbage packet from worker. Should not occur. Send NULL packet back
						Packet reply = new Packet(Packet.PacketType.NULL, -1, null);
						out.writeObject(reply);
						System.err.println("Garbage packet from worker.");
					}
				} catch (Exception e) {
					// Stop looping in case of error
					running = false;
				} finally {
					out.close();
					in.close();
					socket.close();
				}
			} while (running);	
		} catch (FileNotFoundException e) {
			System.err.println("ERROR: File " + filepath + " not found");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			scanner.close();
		}
	}
	
	/**
	 * Internal watcher to check when the primary node gets disconnected.
	 * Retries to become the primary when gets a chance
	 */
	static class PrimeWatch implements Watcher{
		@Override
		public void process(WatchedEvent event) {
			// Check for event type NodeCreated
			boolean isNodeDeleted = event.getType().equals(EventType.NodeDeleted);
			// Verify if this is actually the prime znode
			boolean isPrime = event.getPath().equals(parent_path + "/" + primary);
			if (isNodeDeleted && isPrime) {
				assert(my_path == null);
				System.out.println("Previous prime has died! Attempting to become the prime...");
				// Attempt to create the prime node
				if (registerAsPrime() == false) {
					System.out.println("Didn't become the prime. Will try again later.");
				}
			} else {
				System.out.println("PrimeWatch: Unexpected watch event: " + event.toString());
			}
		}
	}
}
