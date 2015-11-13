package mapreduce;

import java.net.URL;

public interface MapReduceFramework {
	void requestProcess(URL inputURL, URL outputURL, int mappers, int reducers);
}
