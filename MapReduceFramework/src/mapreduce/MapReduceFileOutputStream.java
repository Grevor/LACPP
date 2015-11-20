package mapreduce;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Buffered file stream used in the MapReducer to increase performance.
 * @author Admin
 *
 */
public class MapReduceFileOutputStream extends BufferedOutputStream {
	private static final int BUFFER_SIZE = 1 << 20;
	private FileOutputStream stream;

	public MapReduceFileOutputStream(File file) throws FileNotFoundException {
		super(new FileOutputStream(file), BUFFER_SIZE);
		stream = (FileOutputStream) this.out;
	}
	
	public long getSize() { 
		try {
			return stream.getChannel().position();
		} catch (IOException e) {
			throw new Error("");
		}
	}
}
