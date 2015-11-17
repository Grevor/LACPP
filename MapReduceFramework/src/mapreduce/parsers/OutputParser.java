package mapreduce.parsers;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Interface to an object capable of parsing a class to and from a stream.
 * @author Admin
 *
 * @param <T>
 */
public interface OutputParser<T> {
	T parse(InputStream input);
	void put(OutputStream output, T obj);
}
