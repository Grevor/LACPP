package mapreduce;

import java.io.InputStream;
import java.util.HashMap;

import mapreduce.intermediate.ThreadSpecificEmitter;

public abstract class Mapper<OutKey, OutVal> {
	HashMap<Thread, ThreadSpecificEmitter<OutKey, OutVal>> emitter;
	
	public abstract void map(String key, InputStream file);
	
	public final void emit(OutKey key, OutVal val) {
		emitter.get(Thread.currentThread()).emit(key, val);
	}
	
	final void setEmitter(Thread t, ThreadSpecificEmitter<OutKey, OutVal> emitter) {
		this.emitter.put(t, emitter);
	}
}
