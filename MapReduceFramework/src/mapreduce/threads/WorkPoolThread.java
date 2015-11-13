package mapreduce.threads;

import java.util.concurrent.Semaphore;

import mapreduce.WorkQueue;
import mapreduce.WorkScheduler;

public class WorkPoolThread<Work> extends Thread {
	private int threadIndex;
	private WorkScheduler<Work> scheduler;
	private Semaphore workCompleted;
	
	public WorkPoolThread(Semaphore reporter, WorkScheduler<Work> scheduler, int threadIndex, String description) {
		super(description + " #" + threadIndex);
		this.threadIndex = threadIndex;
		this.scheduler = scheduler;
		workCompleted = reporter;
	}
	
	public WorkQueue<Work> getQueue() { return scheduler.get(threadIndex); }
	public Work attemptSteal() { return scheduler.attemptWorkStealing(threadIndex); }
	public Work tryGetWork() {
		Work work = getQueue().get();
		if(work != null)
			return work;
		
		return attemptSteal();
	}
	public void reportCompletion() {
		if(workCompleted == null)
			return;
		workCompleted.release();
		workCompleted = null;
	}
}
