package Utils;
/* Class Partition represent the file partition split by Master
 * 
 */
public class Partition {
	public String fileName = "";//File name of the parition
	public int start = 0;//start row of the partition
	public int end = -1;//end row of the partition
	
	//Partition constructor
	public Partition(String fileName, int start, int end) {
		this.fileName = fileName;
		this.start = start;
		this.end = end;
	}
}
