package mapreduce;

import java.io.InputStream;
import java.util.HashMap;

import mapreduce.output.OutputStrategy;

public abstract class Mapper<OutKey, OutVal> {
	HashMap<Thread, OutputStrategy<OutKey, OutVal>> emitter = new HashMap<>();
	
	public abstract void map(String key, InputStream file);
	
	public final void emit(OutKey key, OutVal val) {
		emitter.get(Thread.currentThread()).emit(key, val);
	}
	
	final void setEmitter(Thread t, OutputStrategy<OutKey, OutVal> emitter) {
		this.emitter.put(t, emitter);
	}
}
