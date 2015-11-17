package mapreduce.intermediate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

/**
 * Data sorter sorting key-value pairs together.
 * @author Admin
 *
 * @param <Key>
 * @param <Value>
 */
public class IntermediateData<Key, Value> implements IntermediateSorter<Key, Value> {
	private HashMap<Key, Collection<Iterable<Value>>> valueIterators = new HashMap<>();
	
	private Collection<Iterable<Value>> getAllValues(Key key) {
		Collection<Iterable<Value>> col = valueIterators.get(key);
		if(col == null) {
			col = new ArrayList<>();
			valueIterators.put(key, col);
		}
		
		return col;
	}
	@Override
	public Iterable<Key> getKeys(){
		return valueIterators.keySet();
	}
	
	@Override
	public void addKeysAndValues(Key key, Iterable<Value> values) {
		Collection<Iterable<Value>> col = getAllValues(key);
		col.add(values);
	}

	@Override
	public Iterable<Value> getAllValuesForKey(Key k) {
		return new SecondOrderIterable<>(valueIterators.get(k)); 
	}
}
