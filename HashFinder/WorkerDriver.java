import java.io.IOException;

import org.apache.zookeeper.KeeperException;

/**
 * Top level wrapper for worker node
 * Creates a worker node and runs it
 * @author Hasan Imam
 */
public class WorkerDriver {

	/**
	 * @param args <HostIP>:<Port> of the zookeeper 
	 */
	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("Error: Need to provide <Zookeeper Server>:<Port>");
			return;
		}
		try {
			// Create and run a worker
			WorkerNode node1 = new WorkerNode(args[0]);
			node1.run();
			node1.join();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		}
	}
}
