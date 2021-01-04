package Apps;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import Master.Master;

/* Implement User Program to perform MapReduce operation for each Application.
 * User will invoke a Master  to coordinate mappers and reducers.
 */
public class User {
	
	//Entry point of user program:
	//Input:
	//String args[0]: File name of user-defined config 
	//String args[1]: int: Paramters to decide whether to simulate worker fail
	//Output:
	//String outputFiles: File names of output files
	public static void main(String[] args) {
		Master m = new Master(args[0]);
		System.out.println(m.runMapReduce(Integer.valueOf(args[1])));
	}
}
