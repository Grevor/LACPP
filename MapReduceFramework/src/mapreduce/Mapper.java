package mapreduce;

import java.io.InputStream;
import java.util.HashMap;

import mapreduce.output.OutputStrategy;

/**
 * Class representing a mapper function.
 * All classes which is defining the function must use this as their base class.
 * @author Admin
 *
 * @param <OutKey>
 * @param <OutVal>
 */
public abstract class Mapper<OutKey, OutVal> {
	HashMap<Thread, OutputStrategy<OutKey, OutVal>> emitter = new HashMap<>();
	
	/**
	 * Maps the specified Key-File pair to zero or more emitted values.
	 * @param key - The key.
	 * @param file - The stream to get data from.
	 */
	public abstract void map(String key, InputStream file);
	
	/**
	 * Emits a key-value pair to the next step of the algorithm.
	 * @param key - The key to emit.
	 * @param val - The value to emit.
	 */
	public final void emit(OutKey key, OutVal val) {
		emitter.get(Thread.currentThread()).emit(key, val);
	}
	
	/**
	 * Links a thread with an emitter. This may not be done mid-calculation.
	 * @param t - The thread.
	 * @param emitter - The emitter.
	 */
	final void setEmitter(Thread t, OutputStrategy<OutKey, OutVal> emitter) {
		this.emitter.put(t, emitter);
	}
}
