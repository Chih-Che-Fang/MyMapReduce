package Utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/* Class OutputComperator is used to compare the output result with Spark output
 * 
 */
public class OutputComperator {

	/* Function:
	 * Entry point of OutputComperator, will compare the output files with Spark output files
	 * for each of 3 the applications
	 * 
	 * Input:
	 * String args[0]: Number Of Output
	 * Output:
	 * None
	 */
	public static void main(String[] args) {
		String[] outputDirs = {"output_wordcount", "output_urlcount", "output_grep"};
		String[] apps = {"wordcount", "urlcount", "grep"};
		int numOfOutput = Integer.valueOf(args[0]);
		
		for(int k = 0; k < apps.length; ++k) {
			boolean isEqaul = true;
			for(int i = 0; i < numOfOutput; ++i) {
				String file = String.format("%s/out.%s", outputDirs[k], String.valueOf(i));
				String expectedFile = String.format("%s/spark_out.%s", outputDirs[k], String.valueOf(i));
				if(!(isEqaul = compare(file, expectedFile))) {
					break;
				}
			}
			
			System.out.println(String.format("Application:%s", apps[k]));
			System.out.println(String.format("Testing Result: %s", 
					(isEqaul)? "Scuccess, output files matches expected file!" : "Failed, output don't match expected file!"));
			
		}
	}
	
	
	/* Function:
	 * compare the output files with Spark output files
	 * 
	 * Input:
	 * String args[0]: Number Of Output
	 * Output:
	 * boolean res: whether the two files is equal
	 */
	public static boolean compare(String f1, String f2) {
		BufferedReader reader1, reader2;
		try {
			reader1 = new BufferedReader(new FileReader(f1));
			reader2 = new BufferedReader(new FileReader(f2));
			 
			//Compare two files line by line
			String line1 = reader1.readLine();
			String line2 = reader2.readLine();
			while (line1 != null || line2 != null) {
				if(line1 == null || line2 == null || !line1.equals(line2)) return false;
				line1 = reader1.readLine();
				line2 = reader2.readLine();
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return true;
	}
}
