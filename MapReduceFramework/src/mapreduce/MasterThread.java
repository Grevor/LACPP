package mapreduce;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.concurrent.Semaphore;

import mapreduce.intermediate.IntermediateData;
import mapreduce.intermediate.IntermediateSorter;
import mapreduce.intermediate.ThreadSpecificEmitter;
import mapreduce.output.OutputStrategy;
import mapreduce.threads.MapperThread;
import mapreduce.threads.ReducerThread;

public final class MasterThread<InterKey, OutVal, OutKey, OutputVal> extends Thread {
	private URI input, output, mapOutput;
	private Thread master;
	private WorkScheduler<URI> mapScheduler;
	private WorkScheduler<InterKey> reduceScheduler;
	private Semaphore barrier = new Semaphore(0);
	
	private Mapper< InterKey, OutVal> mapper;
	private Reducer<InterKey, OutVal, OutKey, OutputVal> reducer;
	
	public MasterThread(Mapper<InterKey, OutVal> mapper,
			Reducer<InterKey, OutVal, OutKey, OutputVal> reducer,
			URI input, URI output, String id, 
			int mappers, int reducers) {
		super("MapReduce Master Thread '" + id + "'");
		this.input = input;
		this.output = output;
		mapScheduler = new WorkScheduler<>(mappers);
		reduceScheduler = new WorkScheduler<>(reducers);
		this.mapper = mapper;
		this.reducer = reducer;
	}
	
	@Override
	public void run() {
		if(Thread.currentThread() != this)
			throw new IllegalStateException("Can only run a MapReduce Master thread from itself.");
		
		File[] inputFiles = new File(input).listFiles();
		int numberOfFiles = inputFiles.length;
		
		IntermediateSorter<InterKey, OutVal> sortedIntermediateOutput = 
				sortOutput(runMap(inputFiles, numberOfFiles));
		
		runReduce(sortedIntermediateOutput, output);
		
	}

	private IntermediateSorter<InterKey, OutVal> sortOutput(Iterable<ThreadSpecificEmitter<InterKey, OutVal>> runMap) {
		IntermediateSorter<InterKey, OutVal> sorter = new IntermediateData<>();
		for(ThreadSpecificEmitter<InterKey, OutVal> em : runMap) {
			for(InterKey k : em.getKeys())
				sorter.addKeysAndValues(k, em.getValues(k));
		}
		return sorter;
	}

	private void runReduce(IntermediateSorter<InterKey, OutVal> sortedIntermediateOutput, URI output) {
		Iterable<InterKey> keys = sortedIntermediateOutput.getKeys();
		int numKeys = 0;
		for(InterKey k : keys){
			reduceScheduler.addWork(k);
			numKeys++;
		}
		int numThreads = Math.min(numKeys,reduceScheduler.getNumberOfQueues());
		barrier = new Semaphore(numThreads);
		
		ArrayList<ReducerThread<InterKey, OutVal, OutKey, OutputVal>> threads = new ArrayList<>();
		
		for(int i = 0; i < numThreads; i++) {
			OutputStrategy<OutKey, OutputVal> emitter = new ThreadSpecificEmitter<>();
			
			ReducerThread<InterKey, OutVal, OutKey, OutputVal> thread =
					new ReducerThread<>(reducer, barrier, output, reduceScheduler, i, sortedIntermediateOutput);
			reducer.setEmitter(thread,  emitter);
			threads.add(thread);
			
		}
		
		for(int i = 0; i < numThreads; i++)
			threads.get(i).start();
		
		for(int i = 0; i < numThreads; i++)
			barrier.acquireUninterruptibly();

	}

	private Iterable<ThreadSpecificEmitter<InterKey, OutVal>> runMap(File[] inputFiles, int numberOfFiles) {
		//mapOutput = Paths.get(input).resolve();
		for(File f : inputFiles) {
			mapScheduler.addWork(f.toURI());
		}
		
		int numThreads = Math.min(numberOfFiles, mapScheduler.getNumberOfQueues());
		barrier = new Semaphore(numThreads);
		
		ArrayList<MapperThread<InterKey, OutVal>> threads = new ArrayList<>();
		ArrayList<ThreadSpecificEmitter<InterKey, OutVal>> emitters = new ArrayList<>();
		
		for(int i = 0; i < numThreads; i++) {
			ThreadSpecificEmitter<InterKey, OutVal> emitter = new ThreadSpecificEmitter<>();
			emitters.add(emitter);
			
			MapperThread<InterKey, OutVal> thread = 
					new MapperThread<>(mapper, barrier, output, mapScheduler, i);
			mapper.setEmitter(thread, emitter);
			threads.add(thread);
		}
		
		// --- Start and wait for threads.
		for(int i = 0; i < numThreads; i++)
			threads.get(i).start();
		
		for(int i = 0; i < numThreads; i++)
			barrier.acquireUninterruptibly();
		
		return emitters;
	}
}
