package mapreduce;

import java.util.ArrayList;

public class WorkScheduler<Work> {
	private ArrayList<WorkQueue<Work>> pools;
	private int nextQueueToAddWorkTo;
	
	public WorkScheduler(int queues) {
		pools = new ArrayList<>();
		for(int i = 0; i < queues; i++)
			pools.add(new WorkQueue<Work>());
	}
	
	public void addWork(Work work) {
		int index = nextQueueToAddWorkTo;
		nextQueueToAddWorkTo = getNextQueueIndex(nextQueueToAddWorkTo);
		get(index).add(work);
	}
	
	public WorkQueue<Work> get(int index) { return pools.get(index); }
	
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
