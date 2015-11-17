package mapreduce;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A work queue for a single thread.
 * @author Admin
 *
 * @param <Work>
 */
public class WorkQueue<Work> {
	private ConcurrentLinkedQueue<Work> fileURI = new ConcurrentLinkedQueue<>();
	
	public void add(Work file) {
		fileURI.add(file);
	}
	
	public Work get() {
		return fileURI.poll();
	}
}
