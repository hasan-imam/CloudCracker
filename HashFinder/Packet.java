import java.io.Serializable;
import java.util.Vector;

/**
 * Packet class for file server and worker node communication
 * @author Hasan Imam
 */
public class Packet implements Serializable {
	private static final long serialVersionUID = 4619837335009323242L;

	public enum PacketType {
		REQ, // Request from worker
		REPLY, // Valid reply from server
		NULL // Error packet from server
	};
	
	public PacketType type = PacketType.NULL;
	public final int partitionNumber;
	public final Vector<String> partition;
	
	public Packet(PacketType type, int partitionNumber, Vector<String> partition) {
		this.type = type;
		this.partitionNumber = partitionNumber;
		this.partition = partition;
	}
}
