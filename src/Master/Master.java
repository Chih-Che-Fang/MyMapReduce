package Master;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import Utils.UserConfig;

/* Class Master represent master process created by user program, it is responsible for coordinating
 * mappers and reducers and perform the MapReduce operation with fault tolerance mechanism
 */
public class Master {
	UserConfig userConf; //User-defined config
	List<MasterListener> masterListenerList; //List of Master Listener
	ServerSocket serverSocket = null; //Server socker of Master
	static final int PORT = 1234; //Master port
	int simulateFaultyWorker = 0;//0:No faulty node, 1: one faulty mapper, 2:one faulty reducer
	 
	//Function:
	//Master constructor for initialization
	//
	//Input:
	//String userConfigFile: user-defined config
	//Output:
	//None
	public Master(String userConfigFile) {
		this.masterListenerList = new ArrayList<MasterListener>();
		this.userConf = new UserConfig(userConfigFile);
		
		try {
			serverSocket = new ServerSocket(PORT);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		for(int i = 0; i < userConf.numOfMapper; ++i) {
			masterListenerList.add(new MasterListener(String.valueOf(i)));
		}
	}
	
	/* Function:
	 * Perform MapReduce operation 
	 * 
	 * Input:
	 * int simulateFaultyWorker: Whether to simulate faulty nodes
	 * Output:
	 * String outputFiles: file name of output files generated by MapReduce
	 */
	public String runMapReduce(int simulateFaultyWorker) {
		this.simulateFaultyWorker = simulateFaultyWorker;
		String intermediateFiles = perfromMapperJob();	
		return performReducerJob(intermediateFiles);		
	}
	
	/* Function:
	 * Create a independent Reducer process
	 * 
	 * Input:
	 * String s_id: reducer id
	 * String reducerFunction: User-defined reducer function
	 * String intermediateLocs: file locations of intermediate files
	 * String outFile: file name of output files generated by MapReduce
	 * Output:
	 * Process p: reducer process
	 */
	public Process createReducerWorker(String s_id, String reducerFunction, String intermediateLocs, String outFile) {
    	ProcessBuilder pb = new ProcessBuilder("java", "-cp", ".\\bin", "Slave.ReducerWorker",  s_id, 
				reducerFunction, intermediateLocs, outFile);
    	Process p = null;
    	
		pb.directory(new File("."));

		try {
			p = pb.start();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return p;
	}
	
	/* Function:
	 * Create a independent Mapper process
	 * 
	 * Input:
	 * String s_id: mapper id
	 * String mapperFunction: User-defined mapper function
	 * String inputFile: file name of input files
	 * String start: start row of partition file
	 * String end: end row of partition file
	 * String numOfReducer: number of reducer
	 * Output:
	 * Process p: Mapper process
	 */
	public Process createMapperWorker(String s_id, String mapperFunction, String inputFile, 
			String start, String end, String numOfReducer) {
		
    	ProcessBuilder pb = new ProcessBuilder("java", "-cp", ".\\bin", "Slave.MapperWorker",  s_id, 
				mapperFunction, inputFile, start, end,numOfReducer);
    	Process p = null;
    
		pb.directory(new File("."));

		try {
			p = pb.start();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return p;
	}
	
	/* Function:
	 * Perform Reducer Job
	 * 
	 * Input:
	 * String intermediateLocs: locations of intermediate files produced by mappers
	 * Output:
	 * String outputFiles: file name of output files generated by MapReduce
	 */
	public String performReducerJob(String intermediateLocs) {
		String reducerFunction = userConf.reducerFunction;
		StringBuilder res = new StringBuilder();
		
		//Create N reducers and assign intermediate and reducer function to each reducer
		for(int id = 0; id < userConf.numOfReducer; ++id) {

			String s_id = String.valueOf(id);
			String outFile = String.format("%s/out.%s", userConf.outputDir, s_id);
        	Process p = createReducerWorker( s_id, reducerFunction, intermediateLocs, outFile);
			
			try {
				Socket socket = serverSocket.accept();
				System.out.println(String.format("Reducer %s connected",  s_id));
				
				masterListenerList.get(id).startListening(socket);
				
				//Simulate 1 faulty reducer
				if(simulateFaultyWorker == 2) {
					System.out.println("Simulate one faulty reducer!");
					p.destroy();
					simulateFaultyWorker = 0;
				}
				
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			res.append(outFile).append(", ");
		}
		
        //Wait for all Master thread to finish with fault tolerance
		for(int id = 0; id < userConf.numOfReducer; ++id) {
			MasterListener ml = masterListenerList.get(id);
			String s_id = String.valueOf(id);
			String outFile = String.format("%s/out.%s", userConf.outputDir, s_id);
			
			//Implement fault tolerance for fail process
        	while(ml.join().equals("Fail")) {
        		clearOutputFiles(outFile);
        		Process p = createReducerWorker( s_id, reducerFunction, intermediateLocs, outFile);
        		
				try {
					Socket socket = serverSocket.accept();
					System.out.println(String.format("Reducer %s connected",  s_id));
					
					masterListenerList.get(id).startListening(socket);
				} catch (IOException e) {
					e.printStackTrace();
				}
        	}
		}
        
        res.setLength(res.length() - 2);
        return res.toString();
	}
	
	/* Function:
	 * Perform Mapper Job
	 * 
	 * Input:
	 * None
	 * Output:
	 * String intermediateLocs: locations of intermediate files produced by mappers
	 */
	public String perfromMapperJob() {

		//Partition rows in document
		String inputFile = userConf.inputFile;
		int numOfMapper = userConf.numOfMapper;
		int numOfReducer = userConf.numOfReducer;
		String mapperFunction = userConf.mapperFunction;
		Path path = Paths.get(inputFile);
		int lineCount = 0;
		String result = "";
		boolean mappersFinsied = false;
		
        //Initialization
		try {
			lineCount = (int) Files.lines(path).count();
			System.out.println(lineCount);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		//Start mapper job with fault tolerance
		while(!mappersFinsied) {
            //Clean intermediate files
            clearIntermediateFiles("intermediate");
            
           //Create N Mapper and assign partition and mapper function to each mapper
	        for(int id = 0, offset =  lineCount / numOfMapper; id < numOfMapper; ++id) {
	        	int start = id * offset;
	        	int end = (id == numOfMapper - 1)? lineCount : start + offset;
	        	String s_id = String.valueOf(id);
	
	            //Start a new mapper process
				Process p = createMapperWorker(s_id, mapperFunction, inputFile, 
						            			String.valueOf(start), String.valueOf(end),
												String.valueOf(numOfReducer));
				try {
					Socket socket = socket = serverSocket.accept();
					System.out.println(String.format("Mapper %s connected",  s_id));
					
					//Start a listener for each mappers 
					masterListenerList.get(id).startListening(socket);
					
					//Simulate 1 faulty mapper
					if(simulateFaultyWorker == 1) {
						System.out.println("Simulate one faulty mapper!");
						p.destroy();
						simulateFaultyWorker = 0;
					}
					
				} catch (IOException e) {
					e.printStackTrace();
				} 
	        }
	        
	       
	       //Wait for all mappers to finish
	        mappersFinsied = true;
            for(MasterListener ml : masterListenerList) {
            	//Implement faulty tolerance: If any of the mapperWorker failed, re-executed all mappers
            	if((result = ml.join()).equals("Fail")) {
            		System.out.println("Faulty mapper identified!");
            		mappersFinsied = false;
            	}
            }
		}
		
		return result;
	}

	/* Function:
	 * Clear Intermediate Files
	 * 
	 * Input:
	 * String dirName: The directory want to clean
	 * Output:
	 * None
	 */
	public void clearIntermediateFiles(String dirName) {
		File dir = new File(dirName);
		for(File file: dir.listFiles()) {
		    if (!file.isDirectory()) {
		    	System.out.println("Delete file:" + file.getName());
		        file.delete();
		    }
		}
	}
	
	/* Function:
	 * Clear Intermediate Files
	 * 
	 * Input:
	 * String file: The file want to be clean
	 * Output:
	 * None
	 */
	public void clearOutputFiles(String file) {
		File f = new File(file);
		f.delete();
		System.out.println("Delete file:" + f.getName());
	}
}