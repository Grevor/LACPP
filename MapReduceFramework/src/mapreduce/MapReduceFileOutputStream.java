package mapreduce;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

public class MapReduceFileOutputStream extends BufferedOutputStream {
	private static final int BUFFER_SIZE = 1 << 20;

	public MapReduceFileOutputStream(File file) throws FileNotFoundException {
		super(new FileOutputStream(file), BUFFER_SIZE);
	}
}
