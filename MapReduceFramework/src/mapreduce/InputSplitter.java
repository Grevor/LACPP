package mapreduce;

import java.net.URI;

public interface InputSplitter {
	public void splitWork(URI input);
}
