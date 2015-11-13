package mapreduce;

import java.net.URI;

import mapreduce.parsers.OutputParser;

public interface MapReduceFramework {
	<InterKey, InterVal, OutKey, OutVal> StatusTracker requestProcess(
			Mapper<InterKey, InterVal> mapper, 
			Reducer<InterKey, InterVal, OutKey, OutVal> reducer, 
			URI inputURL, URI outputURL, int mappers, int reducers);
	
	<T> void addParser(Class<T> clazz, OutputParser<? extends T> parser);
}
