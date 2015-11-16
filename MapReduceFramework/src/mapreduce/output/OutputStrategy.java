package mapreduce.output;

public interface OutputStrategy<Key, Value> {
	void emit(Key key, Value value);
	Iterable<Key> getKeys();
	Iterable<Value> getValues(Key k);
	void outputComplete();
}
