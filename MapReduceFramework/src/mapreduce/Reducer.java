package mapreduce;

import java.util.HashMap;

import mapreduce.output.OutputStrategy;

public abstract class Reducer<InKey, InVal, OutKey, OutVal> {
	HashMap<Thread, OutputStrategy<OutKey, OutVal>> emitters;
	
	public abstract void reduce(InKey key, Iterable<InVal> values);
	
	public final void emit(OutKey key, OutVal value) {
		emitters.get(Thread.currentThread()).emit(key, value);
	}
	
	final void setEmitter(Thread t, OutputStrategy<OutKey, OutVal> emitter) {
		emitters.put(t, emitter);
	}
}
