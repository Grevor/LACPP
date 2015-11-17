package mapreduce.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Scanner;

import mapreduce.MapReduceFramework;
import mapreduce.SingleNodeMapReduceFramework;
import mapreduce.StatusTracker;
import mapreduce.mappers.EdgeToAdjacencyMapper;
import mapreduce.mappers.WordCountMapper;
import mapreduce.output.InMemoryOutputStrategyFactory;
import mapreduce.output.SingleFileOutputFactory;
import mapreduce.parsers.ArrayListParser;
import mapreduce.parsers.ClassConverter;
import mapreduce.parsers.LongParser;
import mapreduce.parsers.StringParser;
import mapreduce.reducers.EdgeToAdjacencyReducer;
import mapreduce.reducers.WordCountReducer;

public class TestMain {
	private static final HashMap<Integer, TestRunner> excercises = new HashMap<>();
	private static final String outputFilename = "output.txt";
	
	public static void main(String[] args) throws Exception {
		final MapReduceFramework framework = new SingleNodeMapReduceFramework();
		framework.addParser(String.class, StringParser.singleton);
		framework.addParser(Long.class, LongParser.singleton);
		Class<ArrayList<Long>> arrClazz = ClassConverter.convert(ArrayList.class);
		framework.addParser(arrClazz, new ArrayListParser<>(LongParser.singleton, " "));
		
		Scanner scan = new Scanner(System.in);
		System.out.print("Select the number of mapper threads: ");
		final int mappers = scan.nextInt();
		System.out.print("Select the number of reducer threads: ");
		final int reducers = scan.nextInt();
		
		
		
		excercises.put(2, new TestRunner() {
			
			@Override
			public StatusTracker start() {
				File input = new File("input");
				File output = new File("output");
				
				return framework.requestProcess(new WordCountMapper(), 
						new WordCountReducer(), 
						new InMemoryOutputStrategyFactory<String, Long>(), 
						new SingleFileOutputFactory<String, Long>(" ", "\n", outputFilename),
						input.toURI(), output.toURI(), mappers, reducers);
			}
		});
		
		excercises.put(3, new TestRunner() {
			
			@Override
			public StatusTracker start() {
				File input = new File("input3");
				File output = new File("output");
				return framework.requestProcess(new EdgeToAdjacencyMapper(), 
						new EdgeToAdjacencyReducer(),
						new InMemoryOutputStrategyFactory<Long, Collection<Long>>(), 
						new SingleFileOutputFactory<Long, Collection<Long>>(" # ", "\n", outputFilename),
						input.toURI(), output.toURI(), mappers, reducers);
			}
		});
		
		System.out.print("Select the excercise to run: ");
		int ex = scan.nextInt();
		StatusTracker request = excercises.get(ex).start();
		File outputDir = new File("output");
		clearDir(outputDir);
		waitForCompletionAndWriteOutput(outputDir, request);
		scan.close();
	}

	private static void clearDir(File outputDir) {
		for(File f : outputDir.listFiles())
			f.delete();
	}

	private static void waitForCompletionAndWriteOutput(File output, StatusTracker tracker)
			throws FileNotFoundException {
		tracker.waitUntilComplete();
		
		for(File f : output.listFiles()) {
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
