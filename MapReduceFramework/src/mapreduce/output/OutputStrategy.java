package mapreduce.output;

import java.io.IOException;

/**
 * Interface of an object capable of handling output in a MapReduce framework.
 * @author Admin
 *
 * @param <Key>
 * @param <Value>
 */
public interface OutputStrategy<Key, Value> {
	/**
	 * Emits a key-value pair.
	 * @param key - The key to emit.
	 * @param value - The value to emit.
	 */
	void emit(Key key, Value value);
	/**
	 * Gets all keys emitted to this OutputStrategy. Each key may only be present once.
	 * The general contract is that this function can throw errors if outputComplete has yet to be called.
	 * @return
	 * An iterator of all emitted keys.
	 */
	Iterable<Key> getKeys();
	/**
	 * Gets all values for a certain key.
	 * The general contract is that this function can throw errors if outputComplete has yet to be called.
	 * @param k - 
	 * @return
	 */
	Iterable<Value> getValues(Key k);
	void outputComplete() throws IOException;
}
