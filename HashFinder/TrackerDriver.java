/**
 * Top level wrapper for job tracker
 * Creates a job tracker and runs it
 * @author Hasan Imam
 */
public class TrackerDriver {

	/**
	 * @param args <ZookeeperIP:port>
	 */
	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("Error: Need to provide <zkServer:clientPort>");
			return;
		}
		
		JobTracker tracker = new JobTracker(args[0]);
		tracker.run();
		try {
			tracker.isDisconnected.await();
		} catch (InterruptedException e) {
			System.out.print("Interrupted while waiting for connection close.\n");
		}
	}

}
