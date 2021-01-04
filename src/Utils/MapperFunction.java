package Utils;

import java.util.Iterator;
import java.util.Map;

/* Implement Mapper function interface  for user to implement
 * 
 */
@FunctionalInterface
public
interface MapperFunction<T, R> 
{ 
    Iterator<Map.Entry<T, R>> call(T p1, R p2); 
} 
  