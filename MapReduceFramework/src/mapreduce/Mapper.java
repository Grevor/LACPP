package mapreduce;

import java.io.InputStream;

public abstract class Mapper<OutKey, OutVal> {
	public abstract void map(String key, InputStream file);
}
