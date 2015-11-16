package mapreduce.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;

import mapreduce.MapReduceFramework;
import mapreduce.SingleNodeMapReduceFramework;
import mapreduce.StatusTracker;
import mapreduce.mappers.WordCountMapper;
import mapreduce.parsers.ArrayListParser;
import mapreduce.parsers.ClassConverter;
import mapreduce.parsers.LongParser;
import mapreduce.parsers.StringToStringParser;
import mapreduce.reducers.WordCountReducer;

public class TestMain {
	public static void main(String[] args) throws Exception {
		MapReduceFramework framework = new SingleNodeMapReduceFramework();
		framework.addParser(String.class, StringToStringParser.singleton);
		framework.addParser(Long.class, LongParser.singleton);
		Class<ArrayList<Long>> arrClazz = ClassConverter.convert(ArrayList.class);
		framework.addParser(arrClazz, new ArrayListParser<>(LongParser.singleton, " "));
		
		
		File input = new File("input");
		File output = new File("output");
		
		StatusTracker tracker2 = framework.requestProcess(new WordCountMapper(), new WordCountReducer(), 
				input.toURI(), output.toURI(), 3, 3);
	
//		StatusTracker tracker3 = framework.requestProcess(new EdgeToAdjacencyMapper(), new EdgeToAdjacencyReducer(),
//				new InMemoryOutputStrategy<Long, Collection<Long>>(), new SingleFileOutputFactory<Long, Collection<Long>>(" # ", "\n"),
//				input.toURI(), output.toURI(), 2, 2);
		
		waitForCompletionAndWriteOutput(output, tracker2);
	}

	private static void waitForCompletionAndWriteOutput(File output, StatusTracker tracker)
			throws FileNotFoundException {
		tracker.waitUntilComplete();
		File outputFile = output.listFiles()[0];
		
		Scanner scan = new Scanner(outputFile);
		while(scan.hasNextLine()) {
			System.out.println(scan.nextLine());
		}
		scan.close();
	}
}
