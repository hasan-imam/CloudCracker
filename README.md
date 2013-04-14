# CloudCracker

A simple cloud based MD5 hash lookup service. The server framework in divided into 3 parts: 

Job tracker: Tracks jobs coming from clients. 

File server: Servers dictionary partitions to worker nodes.

Worker node: Hash lookup work horse. 

The tracker and the file server can have multiple instances. In that case only one becomes the primary, and others act as backups and can make the system fault tolerant. For example, if the primary tracker or the server crashes, leader election takes place and one of the backups kicks in. These components are mostly stateless by design. The system uses Apache Zookeeper to ensure highly reliable distributed coordination.

The system can have multiple worker nodes running to make the lookup computations faster. Lookup results from different worker nodes are aggregated by the tracker and given back to the client. System currently supports only MD5 hashes, but can easily be augmented to support other types.

Client interface to the system is as a simple command line program. Users need to log in with unique user names. 

#####Client:
Client interface to the system is as a simple command line program. Users need to log in with unique user names. 

1. job [a hash to find] : Adds a job for the given hash
2. query [an already added hash] : Checks the lookup progress of the given hash
3. quit : Quits the client.

A single client can give multiple jobs at to the system (easy to write a script to run batch process). Once a hash has been found, it is (lazily) cached to the local client program to remove clutter on the zookeeper.


#####Steps to deploy the system:
First run the zookeeper server. Check out the zookeeper website: http://zookeeper.apache.org/ for detailed instructions on how to deploy zookeeper (single node or as a cluster). Remember the settings being used to run the server (i.e. the server machine IP and listening port). Other components of the system need these at startup to connect with the zookeeper server.

Sources exists in the HashFinder folder. Following is a list of arguments you need to give to run each component, along with sample commands you can execute to run them while inside HashFinder folder.

1. Job_tracker: 
<br>Arguments: [zookeeper server IP]:[zookeeper server port] 
<br>Example:
java -classpath ../zookeeper/zookeeper-3.3.2.jar:../zookeeper/lib/log4j-1.2.15.jar:../zookeeper/lib/slf4j-api-1.6.1.jar:../zookeeper/lib/slf4j-log4j12-1.6.1.jar:./ TrackerDriver localhost:2181

3. File_server: 
<br>Arguments: [zookeeper server IP]:[zookeeper server port] [file server's listening port] [path to dictionary] [file server's IP] 
<br>Example:
java -classpath ../zookeeper/zookeeper-3.3.2.jar:../zookeeper/lib/log4j-1.2.15.jar:../zookeeper/lib/slf4j-api-1.6.1.jar:../zookeeper/lib/slf4j-log4j12-1.6.1.jar:./ FileServer localhost:2181 9999 dictionary/lowercase.rand localhost

4. Worker_node: 
<br>Arguments: [zookeeper server IP]:[zookeeper server port] 
<br>Example:
java -classpath ../zookeeper/zookeeper-3.3.2.jar:../zookeeper/lib/log4j-1.2.15.jar:../zookeeper/lib/slf4j-api-1.6.1.jar:../zookeeper/lib/slf4j-log4j12-1.6.1.jar:./ WorkerDriver localhost:2181

5. Client: 
<br>Arguments: [zookeeper server IP]:[zookeeper server port] 
<br>Example:
java -classpath ../zookeeper/zookeeper-3.3.2.jar:../zookeeper/lib/log4j-1.2.15.jar:../zookeeper/lib/slf4j-api-1.6.1.jar:../zookeeper/lib/slf4j-log4j12-1.6.1.jar:./ ClientDriver localhost:2181
_________________
