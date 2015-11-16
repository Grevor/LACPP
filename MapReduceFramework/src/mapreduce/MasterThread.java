package mapreduce;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.concurrent.Semaphore;

import mapreduce.intermediate.IntermediateData;
import mapreduce.intermediate.IntermediateSorter;
import mapreduce.output.InMemoryOutputStrategyFactory;
import mapreduce.output.OutputStrategy;
import mapreduce.output.OutputStrategyFactory;
import mapreduce.output.SingleFileOutputFactory;
import mapreduce.parsers.ParserCollection;
import mapreduce.threads.MapperThread;
import mapreduce.threads.ReducerThread;

/**
 * Thread overseeing an invocation of a MapReduce algorithm for a local computation.
 * @author Admin
 *
 * @param <InterKey>
 * @param <InterVal>
 * @param <OutKey>
 * @param <OutVal>
 */
public final class MasterThread<InterKey, InterVal, OutKey, OutVal> extends Thread implements StatusTracker {
	private URI input, output, mapOutput;
	private WorkScheduler<URI> mapScheduler;
	private WorkScheduler<InterKey> reduceScheduler;
	private Semaphore barrier = new Semaphore(0);
	private Semaphore complete = new Semaphore(0);
	
	private OutputStrategyFactory<InterKey, InterVal> mapOutputStrategyFactory = new InMemoryOutputStrategyFactory<>();
	private OutputStrategyFactory<OutKey, OutVal> reduceOutputStrategyFactory = new SingleFileOutputFactory<>();
	
	private ParserCollection parsers;
	
	private Mapper< InterKey, InterVal> mapper;
	private Reducer<InterKey, InterVal, OutKey, OutVal> reducer;
	
	public MasterThread(Mapper<InterKey, InterVal> mapper,
			Reducer<InterKey, InterVal, OutKey, OutVal> reducer,
			OutputStrategyFactory<InterKey, InterVal> mapOutputStrategyFactory,
			OutputStrategyFactory<OutKey, OutVal> reduceOutputStrategyFactory,
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
		this.mapOutputStrategyFactory = mapOutputStrategyFactory;
		this.reduceOutputStrategyFactory = reduceOutputStrategyFactory;
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
	sortOutput(Iterable<OutputStrategy<InterKey, InterVal>> runMap) {
		IntermediateSorter<InterKey, InterVal> sorter = new IntermediateData<>();
		for(OutputStrategy<InterKey, InterVal> em : runMap) {
			for(InterKey k : em.getKeys())
				sorter.addKeysAndValues(k, em.getValues(k));
		}
		return sorter;
	}

	private void runReduce(IntermediateSorter<InterKey, InterVal> sortedIntermediateOutput, URI output) {
		//--- put all work into the scheduler to get ready.
		Iterable<InterKey> keys = sortedIntermediateOutput.getKeys();
		int numKeys = 0;
		for(InterKey k : keys){
			reduceScheduler.addWork(k);
			numKeys++;
		}
		
		// --- Create the barrier we will use.
		int numThreads = Math.min(numKeys,reduceScheduler.getNumberOfQueues());
		barrier = new Semaphore(0);
		
		// --- Create all reducer threads.
		ArrayList<ReducerThread<InterKey, InterVal, OutKey, OutVal>> threads = new ArrayList<>();
		ArrayList<OutputStrategy<OutKey, OutVal>> emitters = new ArrayList<>();
		
		for(int i = 0; i < numThreads; i++) {
			ReducerThread<InterKey, InterVal, OutKey, OutVal> thread =
					new ReducerThread<>(reducer, barrier, output, reduceScheduler, i, sortedIntermediateOutput);
			OutputStrategy<OutKey, OutVal> emitter = createOutputStrategy(output, reduceOutputStrategyFactory);
			reducer.setEmitter(thread,  emitter);
			emitters.add(emitter);
			threads.add(thread);
		}
		
		// --- Start all threads and wait for them to complete.
		for(int i = 0; i < numThreads; i++)
			threads.get(i).start();
		for(int i = 0; i < numThreads; i++)
			barrier.acquireUninterruptibly();
		
		// --- Let the output strategies perform cleanup.
		signalOutputComplete(output, emitters, reduceOutputStrategyFactory);
	}

	/**
	 * Signals all emitters and the factory that output is complete for this step of the algorithm.
	 * @param output - The output URI.
	 * @param emitters - The emitters used to emit data.
	 * @throws Error
	 * If something goes wrong.
	 */
	private <Key, Val> void signalOutputComplete(URI output, Iterable<OutputStrategy<Key, Val>> emitters, OutputStrategyFactory<Key, Val> factory) throws Error {
		try {
			// --- Signal strategies that the output stage is complete.
			for(OutputStrategy<Key, Val> emitter : emitters)
				emitter.outputComplete();
			// --- Signal the strategy factory if additional cleanup is needed.
			factory.outputComplete(output);
		} catch (Exception e) {
			throw new Error("Error finalizing reducer output. Output may be broken, including future output.", e);
		}
	}

	/**
	 * Creates an OutputStrategy from the specified factory.
	 * @param output - The output URI to use.
	 * @param factory - The factory.
	 * @return
	 * @throws Error
	 * If something fails when attempting to create a strategy.
	 */
	private <Key, Val> OutputStrategy<Key, Val> createOutputStrategy(URI output, OutputStrategyFactory<Key, Val> factory)
			throws Error {
		try {
			return factory.create(output, parsers);
		} catch (Exception e) {
			throw new Error("Failed to create output strategy in reducer phase.");
		}
	}

	private Iterable<OutputStrategy<InterKey, InterVal>> runMap(File[] inputFiles, int numberOfFiles) {
		// --- Add all input files as work.
		for(File f : inputFiles) {
			mapScheduler.addWork(f.toURI());
		}
		// --- Initialize the barrier.
		int numThreads = Math.min(numberOfFiles, mapScheduler.getNumberOfQueues());
		barrier = new Semaphore(0);
		
		ArrayList<MapperThread<InterKey, InterVal>> threads = new ArrayList<>();
		ArrayList<OutputStrategy<InterKey, InterVal>> emitters = new ArrayList<>();
		
		for(int i = 0; i < numThreads; i++) {
			OutputStrategy<InterKey, InterVal> emitter = createOutputStrategy(mapOutput, mapOutputStrategyFactory);
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
		
		// --- Signal all outputs that output is complete.
		signalOutputComplete(mapOutput, emitters, mapOutputStrategyFactory);
		return emitters;
	}
	
	@Override
	public void waitUntilComplete() {
		complete.acquireUninterruptibly();
		complete.release();
	}
}
