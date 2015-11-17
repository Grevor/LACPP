package mapreduce;

/**
 * Tracks the status of a request.
 * @author Admin
 *
 */
public interface StatusTracker {
	void waitUntilComplete();
}
