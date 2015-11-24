package mapreduce;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Scanner;

import mapreduce.mappers.CommonFriendsMapper;
import mapreduce.mappers.CountMapper;
import mapreduce.mappers.EdgeToAdjacencyMapper;
import mapreduce.mappers.StringEdgeToAdjacencyMapper;
import mapreduce.mappers.TrianglesMapper;
import mapreduce.mappers.WordCountMapper;
import mapreduce.output.InMemoryOutputStrategyFactory;
import mapreduce.output.MultipleFileOutputFactory;
import mapreduce.output.SingleFileOutputFactory;
import mapreduce.parsers.ArrayListParser;
import mapreduce.parsers.ClassConverter;
import mapreduce.parsers.LongParser;
import mapreduce.parsers.Pair;
import mapreduce.parsers.StringParser;
import mapreduce.reducers.CommonFriendsReducer;
import mapreduce.reducers.CountReducer;
import mapreduce.reducers.EdgeToAdjacencyReducer;
import mapreduce.reducers.StringEdgeToAdjacencyReducer;
import mapreduce.reducers.TrianglesReducer;
import mapreduce.reducers.WordCountReducer;
import mapreduce.test.TestRunner;

public class FrameworkEntryPoint {
	private static final HashMap<Integer, TestRunner> excercises = new HashMap<>();
	private static final String outputFilename = "output.txt";
	
	public static void main(String[] args) throws Exception {
		final String inputPath = args[0];
		final String outputPath = args[1];
		final int mappers = Integer.parseInt(args[2]);
		final int reducers = Integer.parseInt(args[3]);
		final int exercise = Integer.parseInt(args[4]);
		final File input = new File(inputPath);
		final File output = new File(outputPath);
		
		//--- ensures that the output directory exists.
		output.mkdirs();
		clearDir(output);
		
		//--- set up the framework and some needed class constants.
		// Java is silly like that.
		final MapReduceFramework framework = new SingleNodeMapReduceFramework();
		framework.addParser(String.class, StringParser.singleton);
		framework.addParser(Long.class, LongParser.singleton);
		final Class<ArrayList<Long>> arrClazz = ClassConverter.convert(ArrayList.class);
		final Class<ArrayList<String>> arrStrClazz = ClassConverter.convert(ArrayList.class);
		framework.addParser(arrClazz, new ArrayListParser<>(LongParser.singleton, " "));
		
		//--- counts words. Due to UTF-8 limitations, some characters may not be recognized as delimiters.
		excercises.put(2, new TestRunner() {
			@Override
			public StatusTracker start() {
				return framework.requestProcess(new WordCountMapper(), 
						new WordCountReducer(), 
						new InMemoryOutputStrategyFactory<String, Long>(), 
						new SingleFileOutputFactory<String, Long>(" ", "\n", outputFilename),
						input.toURI(), output.toURI(), mappers, reducers);
			}
		});
		
		//--- Maps edge list -> adjacency list
		excercises.put(3, new TestRunner() {
			@Override
			public StatusTracker start() {
				return framework.requestProcess(new EdgeToAdjacencyMapper(), 
						new EdgeToAdjacencyReducer(),
						new InMemoryOutputStrategyFactory<Long, Collection<Long>>(), 
						new SingleFileOutputFactory<Long, Collection<Long>>(" # ", "\n", outputFilename),
						input.toURI(), output.toURI(), mappers, reducers);
			}
		});
		
		//--- common friends.
		// Performs:
		// edge list -> adjacency
		// adjacency -> common friends
		excercises.put(4, new TestRunner() {
			@Override
			public StatusTracker start() {
				framework.addParser(ArrayList.class, new ArrayListParser<>(StringParser.singleton, " "));
				File intermediateOutput = new File("adjacencyOutput");
				intermediateOutput.mkdirs();
				clearDir(intermediateOutput);
				
				StatusTracker adjacency = framework.requestProcess(new StringEdgeToAdjacencyMapper(), 
						new StringEdgeToAdjacencyReducer(),
						new InMemoryOutputStrategyFactory<String, ArrayList<String>>(), 
						new MultipleFileOutputFactory<String, ArrayList<String>>((long)(1<<24), " # ", "\n", String.class, arrStrClazz),
						input.toURI(), intermediateOutput.toURI(), mappers, reducers);
				adjacency.waitUntilComplete();
				return framework.requestProcess(new CommonFriendsMapper(), 
						new CommonFriendsReducer(), 
						new InMemoryOutputStrategyFactory<String, ArrayList<String>>(), 
						new SingleFileOutputFactory<String, ArrayList<String>>(" # ", "\n", "file.txt"), 
						intermediateOutput.toURI(), output.toURI(), mappers, reducers);
			}
		});
		
		//--- Triangles
		// Performs:
		// edge list -> adjacency
		// adjacency -> triangle permutations counts
		// perm counts -> actual count
		excercises.put(5, new TestRunner() {
			@Override
			public StatusTracker start() {
				framework.addParser(ArrayList.class, new ArrayListParser<>(StringParser.singleton, " "));
				
				File intermediateOutput = new File("adjacencyOutput");
				intermediateOutput.mkdirs();
				clearDir(intermediateOutput);
				
				File intermediateOutput2 = new File("countOutput");
				intermediateOutput2.mkdirs();
				clearDir(intermediateOutput2);
				
				StatusTracker operation = framework.requestProcess(new StringEdgeToAdjacencyMapper(), 
						new StringEdgeToAdjacencyReducer(),
						new InMemoryOutputStrategyFactory<String, ArrayList<String>>(), 
						new MultipleFileOutputFactory<String, ArrayList<String>>((long)(1<<24), " ", "\n", String.class, arrStrClazz),//(Class<? extends ArrayList<String>>) ArrayList.class),//(" # ", "\n", outputFilename),
						input.toURI(), intermediateOutput.toURI(), mappers, reducers);
				operation.waitUntilComplete();
				operation = framework.requestProcess(new TrianglesMapper(),
						new TrianglesReducer(),
						new InMemoryOutputStrategyFactory<String, Pair<String, ArrayList<String>>>(), 
						new SingleFileOutputFactory<String, Long>(" ", "\n", "file.txt"), 
						intermediateOutput.toURI(), intermediateOutput2.toURI(), mappers, reducers);
				operation.waitUntilComplete();
				return framework.requestProcess(new CountMapper(),
						new CountReducer(),
						new InMemoryOutputStrategyFactory<String, Long>(), 
						new SingleFileOutputFactory<String, Long>(" ", "\n", "file.txt"), 
						intermediateOutput2.toURI(), output.toURI(), mappers, reducers);
			}
		});
		
		long nanos = timeRequest(exercise, output);
		System.out.println("Time for operation was:\t" + (nanos / 1000000) + "ms");
	}
	
	private static long timeRequest(int ex, File outputDir) throws FileNotFoundException {
		long start = System.nanoTime();
		StatusTracker request = excercises.get(ex).start();
		request.waitUntilComplete();
		long end = System.nanoTime();
		waitForCompletionAndWriteOutput(outputDir, request);
		return end - start;
	}

	private static void clearDir(File outputDir) {
		if(!outputDir.exists())
			return;
		for(File f : outputDir.listFiles())
			f.delete();
	}

	private static void waitForCompletionAndWriteOutput(File output, StatusTracker tracker)
			throws FileNotFoundException {
		tracker.waitUntilComplete();
		File[] outputFiles = output.listFiles();
		if(outputFiles == null) {
			System.out.println("Found no output");
			return;
		}
		
		for(File f : outputFiles) {
			if(f.isDirectory())
				continue;
			
			System.out.println("###");
			System.out.println("#####");
			System.out.println("# Output from '" + f.toURI().toString() + "'");
			System.out.println("#####");
			System.out.println("###");
			Scanner scan = new Scanner(f);
			while(scan.hasNextLine()) {
				System.out.println(scan.nextLine());
			}
			scan.close();
		}
	}
}
