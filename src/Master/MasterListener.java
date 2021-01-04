package Master;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;

/* Class MasterListener represent master a listener that is responsible for communicating with
 * mapper workers or reducer workers through web socket. Each listener is a independent thread and 
 * responsible for communicating a single worker. 
 */
public class MasterListener implements Runnable {

	String id;//Unique identifier for the listener
	Socket socket;//socket for communicating with worker
	protected Thread t;//Listener thread
	protected DataInputStream in =  null;//input stream from worker 
	String result;//job result
	
	//Master listener constructor
	public MasterListener(String id) {
		this.id = id;
	}
	
	/* Function:
	 * Start a listener to communicate with worker
	 * Input:
	 * None
	 * Output:
	 * None
	 */
	public void run() {
		 String msgType = "";
		 this.result = "Fail";
		 
		// takes input from the worker socket 
		 try {
		 	in = new DataInputStream( 
		 		new BufferedInputStream(socket.getInputStream()));
		 	
		 	System.out.println(String.format("Start Master %s Listener", id));
		 	
		 	 //Keep listening messages from work
			 while(!msgType.equals("Over")) {
				 String inputStr = in.readUTF();
				 String[] tokens = inputStr.split(",,");
				 msgType = tokens[0];
				 System.out.println(inputStr);
				 System.out.println(tokens[1]);
				 
				 switch(msgType) {
					 case "Over"://Job finished successfully
						result = tokens[1];
						break;
				 }
				 
			 }
			 
		 } catch (IOException e) {
		 	e.printStackTrace();
		 } 
	}
	
	/* Function:
	 * Start listening a worker
	 * Input:
	 * None
	 * Output:
	 * None
	 */
	public void startListening(Socket socket) {

		this.socket = socket;
		t = new Thread (this, this.id);
		t.start ();
	}
	
	/* Function:
	 * Join the listener thread 
	 * Input:
	 * None
	 * Output:
	 * None
	 */
	public String join() {

		if (t != null) {
			try {
				t.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return result;
	}
	
	/* Function:
	 * Get job Result of worker 
	 * Input:
	 * String output: output of worker
	 * Output:
	 * None
	 */
	public String getResult() {
		return result;
	}
}
