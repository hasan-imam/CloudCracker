import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Class used to store client data in client nodes. Convenient compared to string parsing
 * Light weight data structure since it will be stored in znode (limit 1 MB)
 * Not intended to have any strict access control
 */
public class ClientData implements Serializable {

	private static final long serialVersionUID = -4522653073661827034L;

	public enum JobStat {
		NEW, // Newly added job. Tracker has not put it in the job pool yet
		PROCESSING, // Job that has been put in the job pool where workers look for jobs
		DONE // Finished job. Set to Done only after the tacker/other managing entity has created the result node
	};
	
	/**
	 * Stores pairs of job information for this client
	 * Format: <key, value>. Key = job name, value = current status
	 */
	public Map<String, JobInfo> dataMap = new HashMap <String, JobInfo>();
	
	public JobInfo getJobInfo (String hash) {
		if (hash == null)
			return null;
		return dataMap.get(hash);
	}
	
	public String getJobResult (String hash) {
		if (hash == null)
			return null;
		JobInfo temp = dataMap.get(hash);
		if (temp != null) {
			return temp.result;
		}
		return null;
	}
	
	public JobStat getJobType (String hash) {
		if (hash == null)
			return null;
		JobInfo temp = dataMap.get(hash);
		if (temp != null) {
			return temp.type;
		}
		return null;
	}
	
//	/**
//	 * Stores pairs of job hash string and their respective results
//	 * Format: <key, value>. Key = job name (hash), value = result
//	 * Data inserted here only by the job tracker when a job has been finished
//	 */
//	public Map<String, String> results = new HashMap <String, String>();
}

class JobInfo implements Serializable {
	private static final long serialVersionUID = -1252041384240697703L;
	public String result = null;
	public ClientData.JobStat type = null;
	
	public JobInfo(ClientData.JobStat type) {
		this.type = type;
	}
}
