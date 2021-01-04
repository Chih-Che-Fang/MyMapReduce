package Utils;

import java.util.Iterator;
/* Implement Reducer function interface  for user to implement
 * 
 */
@FunctionalInterface
public
interface ReducerFunction<T, R> 
{ 
	R call(T p1, Iterator<R> p2); 
} 