package Slave;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import Utils.MapperFunction;
import Utils.Partition;

/* Class MapperWorker represent a mapper process created by master, it is responsible for 
 * performing mapper job according to the user-defined mapper function and its own partition.
 * It will generate intermediate files for the partition and notify Master the locations.
 * Once mapper job is finished, it will pass "Over" to the master listener to inform the master.
 */
public class MapperWorker extends Worker {
	MapperFunction mapperFunction;//User-defined mapper function
	List<Map.Entry<String, String>>[] intermediates;//In-memory intermediate file for temporary use
	String[] intermediateLocs;//Locations of intermediate files
	Partition partition;//File partition for the mapper
	

	/* Function:
	 * MapperWorker Constructor
	 * 
	 * Input:
	 * String id: Unique identifier for the mapper
	 * String mapperFunction: User-defined mapper function class
	 * Partition partition: Assiged parition for the mapper
	 * int numOfReducer: Number Of Reducer
	 * Output:
	 * None
	 */
	public MapperWorker(String id, String mapperFunction, Partition partition, int numOfReducer) {
		super();
		try {
			//Initialize member variables
			Class myClass = Class.forName(mapperFunction);
			Class[] types = {};
			Constructor constructor = myClass.getConstructor(types);
			Object[] parameters = {};
			
			this.mapperFunction = (MapperFunction) constructor.newInstance(parameters);
			this.partition = partition;
			this.id = id;
			this.numOfReducer = numOfReducer;
			intermediates = new List[numOfReducer];
			intermediateLocs = new String[numOfReducer];

			for(int i = 0; i < intermediates.length; ++i) {
				intermediates[i] = new ArrayList<Map.Entry<String, String>>();
				intermediateLocs[i] = String.format("intermediate/tmp.%s", String.valueOf(i));
			}
			
		} catch (ClassNotFoundException e1) {
			outputLog(e1.toString(), id);
		} catch (Exception e) {
			outputLog(e.toString(), id);
		}
	}
	
	/* Function:
	 * Print input Args for debug
	 * 
	 * Input:
	 * String[] args: input args
	 * Output:
	 * String res: joined string of input args
	 */
	public static String outputArgs(String[] args) {
		StringBuilder res = new StringBuilder();
		for(String s : args) {
			res.append(s).append(" ");
		}
		res.setLength(res.length() - 1);
		return res.toString();
	}
	
	/* Function:
	 * Output Mapper Log to Master Listener
	 * 
	 * Input:
	 * String log: Content of log
	 * String id: Unique identifier for the mapper
	 * Output:
	 * None
	 */
	public static void outputLog(String log, String id) {
		try {
			out.writeUTF(String.format("Log,,Mapper %s: %s", id, log));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/* Function:
	 * Entry point of mapper process, will create a mapper to perform 
	 * maper jobs and return result to master
	 * 
	 * Input:
	 * String args[0]: mapper id
	 * String args[1]: User-defined mapper function
	 * String args[2]: file name of input files
	 * String args[3]: start row of partition file
	 * String args[4]: end row of partition file
	 * String args[5]: number of reducer
	 * Output:
	 * None
	 */
	public static void main(String[] args) {
		//Initialization network socket
        try { 
            socket = new Socket(masterAddress, masterPort);
            System.out.println("Mapper Connected");
            
            input  = new DataInputStream(System.in); 
            out    = new DataOutputStream(socket.getOutputStream()); 
        } catch(UnknownHostException u) { 
            System.out.println(u); 
        } catch(IOException i) { 
            System.out.println(i); 
        }
        

		//Create MapperWorker
		MapperWorker 	mapperWorker = new MapperWorker(args[0], args[1], 
				new Partition(args[2], Integer.valueOf(args[3]), Integer.valueOf(args[4])), 
				Integer.valueOf(args[5]));;
			
		outputLog(outputArgs(args), mapperWorker.id);
		
		//Perform mapper job
		mapperWorker.run();
      
        //Notify the master that mapper job is finished
        try
        { 
        	Thread.sleep(1000);
        	out.writeUTF(String.format("Over,,%s", mapperWorker.getIntermediateLocs()));
        	Thread.sleep(2000);
            input.close(); 
            //out.close(); 
            //socket.close(); 
        } 
        catch(IOException | InterruptedException i) 
        { 
            System.out.println(i); 
        } 
	}

	/* Function:
	 * Perform mapper job
	 * 
	 * Input:
	 * Noe
	 * Output:
	 * None
	 */
	public void run() {
		map();
		outputMap();
	}
	
	
	/* Function:
	 * Perform mapper function to map input value
	 * 
	 * Input:
	 * Noe
	 * Output:
	 * None
	 */
	public void map() {
		//For each parition, perform mapper function
		try (BufferedReader reader = Files.newBufferedReader(
		        Paths.get(partition.fileName), StandardCharsets.UTF_8)) {
		    List<String> lines = reader.lines()
		                              .skip(partition.start)
		                              .limit(partition.end - partition.start)
		                              .collect(Collectors.toList());

		    for(String l : lines) {
		    	outputLog(String.format("Read Line:%s", l), id);
		    	storeMap(this.mapperFunction.call(partition.fileName, l));
		    }
		    
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/* Function:
	 * Store each single output of mapper function to memory
	 * 
	 * Input:
	 * Iterator<Map.Entry<String, String>> it: Iterator of <key,value> pairs generated by Mapper function
	 * Output:
	 * None
	 */
	public void storeMap(Iterator<Map.Entry<String, String>> it) {
		while(it.hasNext()) {
			Map.Entry<String, String> entry = it.next();
			//System.out.println(String.format("pair: %s %s", entry.getKey(), entry.getValue()));
			//System.out.println(entry.getKey().hashCode());
			
			//Map each key to one of the intermediate file
			intermediates[(entry.getKey().hashCode() & 0xfffffff) % this.numOfReducer].add(entry);
		}
	}
	
	/* Function:
	 * Output the stored <key,value> pairs to intermediate files
	 * 
	 * Input:
	 * None
	 * Output:
	 * None
	 */
	public void outputMap() {
		FileWriter fstream;
		BufferedWriter out;
 
		//For each itermediate file, output the <key,value> pairs to the mapped intermediate file
		for(int i = 0; i < intermediates.length; ++i) {
			System.out.println("Intermediate:" + i);
			try {
				System.out.println("Output intermediate to loc:" + intermediateLocs[i]);
				fstream =  new FileWriter(intermediateLocs[i], true);
				out = new BufferedWriter(fstream);

				for(Map.Entry<String, String> entry : intermediates[i]) {
					//System.out.println(String.format("pair: %s %s", entry.getKey(), entry.getValue()));
					if(!entry.getKey().equals("")) {
						out.write(entry.getKey());
						out.newLine();
						out.write(entry.getValue());
						out.newLine();
					}
				}		
				out.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/* Function:
	 * Access locations of intermediates
	 * 
	 * Input:
	 * None
	 * Output:
	 * String res: locations of intermediate files
	 */
	public String getIntermediateLocs() {
		StringBuilder res = new StringBuilder();
		for(String loc : this.intermediateLocs) {
			res.append(loc).append(',');
		}
		res.setLength(res.length() - 1);
		return res.toString();
	}
}
