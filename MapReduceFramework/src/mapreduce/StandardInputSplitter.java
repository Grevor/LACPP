package mapreduce;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;

public class StandardInputSplitter implements InputSplitter {
	private static final long SPLIT_SIZE = 1 << 26;

	@Override
	public void splitWork(URI input) {
		File directory = new File(input);
		if(!directory.exists() || !directory.isDirectory())
			throw new Error("Input directory does not exist.");
		File[] files =  directory.listFiles();
		ArrayList<File> acceptedNames = new ArrayList<>();
		
		for(File f : files) {
			if(f.getTotalSpace() <= SPLIT_SIZE)
				continue;
			
			
			InputStream stream;
			try {
				stream = new MapReduceFileInputStream(f);
				int numberOfFiles = (int) Math.ceil(f.getTotalSpace() / SPLIT_SIZE);
				
				stream.close();
			} catch (FileNotFoundException e) {
				throw new Error("Missing file that is listed.");
			} catch (IOException e) {
				throw new Error("Failed to close file.");
			}
			
		}
	}

}
