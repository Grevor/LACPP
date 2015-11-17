package mapreduce;

import java.util.ArrayList;

/**
 * Work scheduler, containing a number of work queues. Work stealing is possible.
 * @author Admin
 *
 * @param <Work>
 */
public class WorkScheduler<Work> {
	private ArrayList<WorkQueue<Work>> pools;
	private int nextQueueToAddWorkTo;
	
	public WorkScheduler(int queues) {
		pools = new ArrayList<>();
		for(int i = 0; i < queues; i++)
			pools.add(new WorkQueue<Work>());
	}
	
	/**
	 * Adds work to a queue in this scheduler.
	 * @param work - The work to add.
	 */
	public void addWork(Work work) {
		int index = nextQueueToAddWorkTo;
		nextQueueToAddWorkTo = getNextQueueIndex(nextQueueToAddWorkTo);
		get(index).add(work);
	}
	
	/**
	 * Gets a certain queue from this scheduler.
	 * @param index - The index of the queue.
	 * @return
	 * The queue.
	 */
	public WorkQueue<Work> get(int index) { return pools.get(index); }
	
	/**
	 * Attempts to steal work from another queue.
	 * @param index - The index of the queue NOT to steal from.
	 * @return
	 * The stealed work. If null, no work could be stolen.
	 */
	public Work attemptWorkStealing(int index) {
		int iterations = 1;
		for(; iterations < pools.size(); iterations++, index = getNextQueueIndex(index)) {
			Work work = get(index).get();
			if(work == null)
				continue;
			
			return work;
		}
		return null;
	}
	
	private int getNextQueueIndex(int current) {
		return (current + 1) % pools.size();
	}

	public int getNumberOfQueues() { return pools.size(); }
}
