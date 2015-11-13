package mapreduce;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

public class MapReduceFileInputStream extends BufferedInputStream {
	private static final int BUFFER_SIZE = 1 << 20;

	public MapReduceFileInputStream(File file) throws FileNotFoundException {
		super(new FileInputStream(file), BUFFER_SIZE);
	}
}
