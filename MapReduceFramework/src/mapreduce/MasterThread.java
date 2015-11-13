package mapreduce;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.concurrent.Semaphore;

import mapreduce.intermediate.IntermediateData;
import mapreduce.intermediate.IntermediateSorter;
import mapreduce.intermediate.ThreadSpecificEmitter;
import mapreduce.output.OutputStrategy;
import mapreduce.output.SingleFileOutput;
import mapreduce.parsers.ParserCollection;
import mapreduce.threads.MapperThread;
import mapreduce.threads.ReducerThread;

public final class MasterThread<InterKey, InterVal, OutKey, OutVal> extends Thread implements StatusTracker {
	private URI input, output, mapOutput;
	private WorkScheduler<URI> mapScheduler;
	private WorkScheduler<InterKey> reduceScheduler;
	private Semaphore barrier = new Semaphore(0);
	private Semaphore complete = new Semaphore(0);
	
	private ParserCollection parsers;
	
	private Mapper< InterKey, InterVal> mapper;
	private Reducer<InterKey, InterVal, OutKey, OutVal> reducer;
	
	public MasterThread(Mapper<InterKey, InterVal> mapper,
			Reducer<InterKey, InterVal, OutKey, OutVal> reducer,
			URI input, URI output, String id, 
			int mappers, int reducers,
			ParserCollection parsers) {
		super("MapReduce Master Thread '" + id + "'");
		this.input = input;
		this.output = output;
		mapScheduler = new WorkScheduler<>(mappers);
		reduceScheduler = new WorkScheduler<>(reducers);
		this.mapper = mapper;
		this.reducer = reducer;
		this.parsers = parsers;
	}
	
	@Override
	public void run() {
		if(Thread.currentThread() != this)
			throw new IllegalStateException("Can only run a MapReduce Master thread from itself.");
		
		File inputDirectory = new File(input);
		if(!inputDirectory.isDirectory())
			throw new Error("Input directory is invalid.");
		
		File[] inputFiles = inputDirectory.listFiles();
		int numberOfFiles = inputFiles.length;
		
		IntermediateSorter<InterKey, InterVal> sortedIntermediateOutput = 
				sortOutput(runMap(inputFiles, numberOfFiles));
		
		runReduce(sortedIntermediateOutput, output);
		complete.release(Integer.MAX_VALUE);
	}

	private IntermediateSorter<InterKey, InterVal> 
	sortOutput(Iterable<ThreadSpecificEmitter<InterKey, InterVal>> runMap) {
		IntermediateSorter<InterKey, InterVal> sorter = new IntermediateData<>();
		for(ThreadSpecificEmitter<InterKey, InterVal> em : runMap) {
			for(InterKey k : em.getKeys())
				sorter.addKeysAndValues(k, em.getValues(k));
		}
		return sorter;
	}

	private void runReduce(IntermediateSorter<InterKey, InterVal> sortedIntermediateOutput, URI output) {
		Iterable<InterKey> keys = sortedIntermediateOutput.getKeys();
		int numKeys = 0;
		for(InterKey k : keys){
			reduceScheduler.addWork(k);
			numKeys++;
		}
		int numThreads = Math.min(numKeys,reduceScheduler.getNumberOfQueues());
		barrier = new Semaphore(0);
		
		OutputStream outputStream = null;
		try {
			URI actualOutputURI = output.resolve("ALMIGHTY.txt");
			File actualOutputFile = new File(actualOutputURI);
			actualOutputFile.createNewFile();
			outputStream = new MapReduceFileOutputStream(actualOutputFile);
		} catch (IOException e) {
			throw new Error("Failed to create or open output file.");
		}
		
		ArrayList<ReducerThread<InterKey, InterVal, OutKey, OutVal>> threads = new ArrayList<>();
		OutputStrategy<OutKey, OutVal> emitter = new SingleFileOutput<OutKey, OutVal>(outputStream, parsers);
		for(int i = 0; i < numThreads; i++) {
			ReducerThread<InterKey, InterVal, OutKey, OutVal> thread =
					new ReducerThread<>(reducer, barrier, output, reduceScheduler, i, sortedIntermediateOutput);
			reducer.setEmitter(thread,  emitter);
			threads.add(thread);
			
		}
		
		for(int i = 0; i < numThreads; i++)
			threads.get(i).start();
		
		for(int i = 0; i < numThreads; i++)
			barrier.acquireUninterruptibly();

		try {
			outputStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private Iterable<ThreadSpecificEmitter<InterKey, InterVal>> runMap(File[] inputFiles, int numberOfFiles) {
		//mapOutput = Paths.get(input).resolve();
		for(File f : inputFiles) {
			mapScheduler.addWork(f.toURI());
		}
		
		int numThreads = Math.min(numberOfFiles, mapScheduler.getNumberOfQueues());
		barrier = new Semaphore(0);
		
		ArrayList<MapperThread<InterKey, InterVal>> threads = new ArrayList<>();
		ArrayList<ThreadSpecificEmitter<InterKey, InterVal>> emitters = new ArrayList<>();
		
		for(int i = 0; i < numThreads; i++) {
			ThreadSpecificEmitter<InterKey, InterVal> emitter = new ThreadSpecificEmitter<>();
			emitters.add(emitter);
			
			MapperThread<InterKey, InterVal> thread = 
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
	
	@Override
	public void waitUntilComplete() {
		complete.acquireUninterruptibly();
	}
}
