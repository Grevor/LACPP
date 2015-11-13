package mapreduce.output;

public interface OutputStrategy<Key, Value> {
	void emit(Key key, Value value);
}
