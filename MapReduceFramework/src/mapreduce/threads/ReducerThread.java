package mapreduce.threads;

import java.net.URI;
import java.util.concurrent.Semaphore;

import mapreduce.Reducer;
import mapreduce.WorkScheduler;
import mapreduce.intermediate.IntermediateSorter;

/**
 * Thread running a reduce process in the MapReduce framework.
 * @author Admin
 *
 * @param <InterKey>
 * @param <InterVal>
 * @param <OutKey>
 * @param <OutputVal>
 */
public class ReducerThread<InterKey, InterVal, OutKey, OutputVal> extends WorkPoolThread<InterKey>{
	
	private static final String desc = "Reducer Thread";
	private URI output;
	private Reducer<InterKey, InterVal, OutKey, OutputVal> reducer;
	private IntermediateSorter<InterKey, InterVal> sorter;
	
	public ReducerThread(Reducer<InterKey, InterVal, OutKey, OutputVal> reducer,
			Semaphore reporter, URI output, 
			WorkScheduler<InterKey> scheduler, int threadIndex, IntermediateSorter<InterKey,InterVal> sorter) 
	
	{
		super(reporter, scheduler, threadIndex, desc);
		this.output = output;
		this.reducer = reducer;
		this.sorter = sorter;
	}
	
	@Override
	public void run() {
		while(true){
			InterKey key = tryGetWork();
			if(key==null)
				break;
			Iterable<InterVal> iter = sorter.getAllValuesForKey(key);
			reducer.reduce(key, iter);
		}
		reportCompletion();
	}

}
