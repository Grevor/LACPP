package mapreduce;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Class creating URI:s in a certain directory.
 * @author Admin
 *
 */
public class OutputURICreator {
	private Path directoryPath;
	private int fileNumber;
	
	public OutputURICreator(URI base) {
		directoryPath = Paths.get(base);
	}
	
	public URI getOutputURI() {
		URI uri = directoryPath.resolve("output" + fileNumber).toUri();
		fileNumber++;
		return uri;
	}
}
