package mapreduce;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Class creating URI:s in a certain directory.
 * @author Admin
 *
 */
public class OutputURICreator {
	private Path directoryPath;
	private AtomicLong fileNumber = new AtomicLong();
	
	public OutputURICreator(URI base) {
		directoryPath = Paths.get(base);
	}
	
	public URI getOutputURI() {
		long number = fileNumber.getAndIncrement();
		URI uri = directoryPath.resolve("output" + number).toUri();
		return uri;
	}
}
