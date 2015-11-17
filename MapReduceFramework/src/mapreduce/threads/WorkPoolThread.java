package mapreduce.threads;

import java.util.concurrent.Semaphore;

import mapreduce.WorkQueue;
import mapreduce.WorkScheduler;

/**
 * Thread working in a workpool. The thread has a queue index and a scheduler to get work from.
 * @author Admin
 *
 * @param <Work>
 */
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
	
	/**
	 * Gets this threads work queue.
	 * @return
	 * The queue.
	 */
	public WorkQueue<Work> getQueue() { return scheduler.get(threadIndex); }
	/**
	 * Attempts to steal work from the scheduler, regardless of wether this threads queue is empty or not.
	 * @return
	 * The stolen work. If null, no work was stolen.
	 */
	public Work attemptSteal() { return scheduler.attemptWorkStealing(threadIndex); }
	/**
	 * Attempts to get work to do, possibly stealing if the own work queue is empty.
	 * @return
	 * The work. If null, no work could be found.
	 */
	public Work tryGetWork() {
		Work work = getQueue().get();
		if(work != null)
			return work;
		
		return attemptSteal();
	}
	/**
	 * Reports that the thread could not find any more work to do.
	 */
	public void reportCompletion() {
		if(workCompleted == null)
			return;
		workCompleted.release();
		workCompleted = null;
	}
}
