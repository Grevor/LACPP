package mapreduce.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

import mapreduce.MapReduceFramework;
import mapreduce.SingleNodeMapReduceFramework;
import mapreduce.StatusTracker;
import mapreduce.mappers.WordCountMapper;
import mapreduce.parsers.LongToStringParser;
import mapreduce.parsers.StringToStringParser;
import mapreduce.reducers.WordCountReducer;

public class TestMain {
	public static void main(String[] args) throws Exception {
		MapReduceFramework framework = new SingleNodeMapReduceFramework();
		framework.addParser(String.class, StringToStringParser.singleton);
		framework.addParser(Long.class, LongToStringParser.singleton);
		
		
		File input = new File("input");
		File output = new File("output");
		String abs = input.getAbsolutePath();
		
		StatusTracker tracker = framework.requestProcess(new WordCountMapper(), new WordCountReducer(), 
				input.toURI(), output.toURI(), 3, 3);
	
		tracker.waitUntilComplete();
		File outputFile = output.listFiles()[0];
		
		Scanner scan = new Scanner(outputFile);
		while(scan.hasNextLine()) {
			System.out.println(scan.nextLine());
		}
		scan.close();
	}
}
