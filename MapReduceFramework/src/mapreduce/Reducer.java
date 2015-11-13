package mapreduce;

import java.util.Iterator;

public abstract class Reducer<InKey, InVal, OutKey, OutVal> {
	abstract void reduce(InKey key, Iterator<InVal> values);
	
}
