package mapreduce.intermediate;

public interface IntermediateSorter<Key, Value> {
	void addKeysAndValues(Key key, Iterable<Value> values);
	
	Iterable<Value> getAllValuesForKey(Key k);

	Iterable<Key> getKeys();
}
