package mapreduce;

import java.util.concurrent.ConcurrentLinkedQueue;

public class WorkQueue<Work> {
	private ConcurrentLinkedQueue<Work> fileURI = new ConcurrentLinkedQueue<>();
	
	public void add(Work file) {
		fileURI.add(file);
	}
	
	public Work get() {
		return fileURI.poll();
	}
}
