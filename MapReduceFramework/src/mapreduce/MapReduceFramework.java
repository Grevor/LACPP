package mapreduce;

import java.net.URI;

public interface MapReduceFramework {
	void requestProcess(URI inputURL, URI outputURL, int mappers, int reducers);
}
