package mapreduce;

import java.io.File;
import java.net.URI;
import java.nio.file.Paths;
import java.util.concurrent.Semaphore;

import mapreduce.threads.MapperThread;

public final class MasterThread<OutKey, OutVal> extends Thread {
	private URI input, output, mapOutput;
	private Thread master;
	private WorkScheduler<URI> mapScheduler;
	private WorkScheduler<URI> reduceScheduler;
	private Semaphore barrier = new Semaphore(0);
	
	private Mapper< OutKey, OutVal> mapper;
	
	public MasterThread(Mapper<OutKey, OutVal> mapper,
			URI input, URI output, String id, int mappers, int reducers) {
		super("MapReduce Master Thread '" + id + "'");
		this.input = input;
		this.output = output;
		mapScheduler = new WorkScheduler<>(mappers);
		reduceScheduler = new WorkScheduler<>(reducers);
		this.mapper = mapper;
	}
	
	@Override
	public void run() {
		if(Thread.currentThread() != this)
			throw new IllegalStateException("Can only run a MapReduce Master thread from itself.");
		
		File[] inputFiles = new File(input).listFiles();
		int numberOfFiles = inputFiles.length;
		
		runMap(inputFiles, numberOfFiles);
		runCombine(output);
		runReduce(output);
		
	}

	private void runMap(File[] inputFiles, int numberOfFiles) {
		mapOutput = Paths.get(input).resolve();
		for(File f : inputFiles) {
			mapScheduler.addWork(f.toURI());
		}
		
		int numThreads = Math.min(numberOfFiles, mapScheduler.getNumberOfQueues());
		barrier = new Semaphore(numThreads);
		
		for(int i = 0; i < numThreads; i++)
			new MapperThread<OutKey, OutVal>(mapper, barrier, output, mapScheduler, i).start();
		
		for(int i = 0; i < numThreads; i++)
			barrier.acquireUninterruptibly();
	}
}
