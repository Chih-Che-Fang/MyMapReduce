package Utils;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/*  Class UserConfig represent the user-defined config file, it includes attributes:
 *  appName: User Application's name
 *  numOfMapper: Number of Mappers
 *  numOfReducer: Number of Reducers
 *  inputFile: Path and file name for input file
 *  outputDir: Path for output directory
 *  mapperFunction:  Class name of reducer function for the application
 *  reducerFunction: Class name of mapper function for the application
 */
public class UserConfig {
	public int numOfMapper = 1;
	public int numOfReducer = 1;
	public String inputFile = "input";
	public String outputDir = "output";
	public String appName = "";
	public String mapperFunction = "";
	public String reducerFunction = "";
	
	//Read user-defined config and store user-defined parameter
	public UserConfig(String userConfigFile) {
		BufferedReader reader;
		try {
			reader = new BufferedReader(new FileReader(userConfigFile));
			String line = reader.readLine();
			while (line != null) {
				String[] tokens = line.split(",");
				switch(tokens[0]) {
					case "appName":
						this.appName = tokens[1];
					break;
					case "numOfMapper":
						this.numOfMapper = Integer.valueOf(tokens[1]);
					break;
					case "numOfReducer":
						this.numOfReducer = Integer.valueOf(tokens[1]);
					break;
					case "inputFile":
						this.inputFile = tokens[1];
					break;
					case "outputDir":
						this.outputDir = tokens[1];
					break;
					case "mapperFunction":
						this.mapperFunction = tokens[1];
					break;
					case "reducerFunction":
						this.reducerFunction = tokens[1];
					break;
				}
				System.out.println(line);
				line = reader.readLine();
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
