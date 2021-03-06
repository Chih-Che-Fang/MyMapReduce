package Apps;
import java.util.Iterator;
import Utils.ReducerFunction;

/* Implement Reducer function class for Distributed Grep Application
 * 
 *  Distributed Grep: The map function emits a line if it
 *	matches a supplied pattern. The reduce function is an
 *	identity function that just copies the supplied intermediate
 *	data to the output.
 */

public class GrepReducerFunction implements ReducerFunction<String,String> {

	/* Function:
	 * Reducer function for "Distributed Grep" Application
	 * Input:
	 * String Key: document
	 * String Value: all lines matched the pattern in the document
	 * Output:
	 * Iterator<Map.Entry<String, String>> res: <key,value> pairs generated by value
	 */
	@Override
	 public String call(String s, Iterator<String> it) {
		 StringBuilder res = new StringBuilder("\n");
		 
		 while(it.hasNext()) {
			 res.append(it.next()).append("\n");
		 }
		 return res.toString();
	 }
}
