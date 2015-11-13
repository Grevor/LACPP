package mapreduce.parsers;

import java.io.InputStream;
import java.io.OutputStream;

public interface OutputParser<T> {
	T parse(InputStream input);
	void put(OutputStream output, T obj);
}
