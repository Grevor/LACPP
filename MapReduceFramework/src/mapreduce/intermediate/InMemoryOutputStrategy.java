package mapreduce.intermediate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import mapreduce.output.OutputStrategy;

public class InMemoryOutputStrategy<Key, Value> implements OutputStrategy<Key, Value> {
	private HashMap<Key, Collection<Value>> values = new HashMap<>();
	
	private Collection<Value> getValuesForKey(Key k) {
		Collection<Value> col = values.get(k);
		if(col == null) {
			col = new ArrayList<>();
			values.put(k, col);
		}
		return col;
	}
	
	public Iterable<Key> getKeys() { return values.keySet(); }
	public Iterable<Value> getValues(Key k) { return values.get(k); }
	
	@Override
	public void emit(Key key, Value val) {
		Collection<Value> values = getValuesForKey(key);
		values.add(val);
	}

	@Override
	public void outputComplete() { }
}
