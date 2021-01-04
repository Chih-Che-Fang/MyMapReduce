package Slave;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/* Class Worker represent a abstract class for mapper and reducer workers. It defines common attributes and
 * virtual function for mapper and reducer worker to inheritance
 */
public class Worker implements Runnable {

	String id = ""; //Unique identifier of the worker
	int numOfReducer = 1; //Number Of Reducer
	protected static Socket socket            = null; //Web socket to communicate with master
	protected static DataInputStream  input   = null; //Input stream from master
	protected static DataOutputStream out     = null; //Output stream to master 
	protected static final String masterAddress = "127.0.0.1"; //IPv4 address of master 
	protected static final int masterPort = 1234; //port of master
	
	//Virtual function
	public void run() {
		
	}
}
