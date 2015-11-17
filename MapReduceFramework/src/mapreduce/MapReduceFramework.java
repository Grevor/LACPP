package mapreduce;

import java.net.URI;

import mapreduce.output.OutputStrategyFactory;
import mapreduce.parsers.OutputParser;

/**
 * Interface to a MapReduce framework.
 * This framework makes no guarantees as to how it runs, but will ensure the requested operations are carried out if possible.
 * @author Admin
 *
 */
public interface MapReduceFramework {
	/**
	 * Requests that a certain map-reduce algorithm is run.<br>
	 * A default strategy will be used for both intermediate and final output.
	 * @param mapper - The mapper to use.
	 * @param reducer - The reducer to use.
	 * @param inputURL - The URI of the input files.
	 * @param outputURL - The URI where the output will be stored.
	 * @param mappers - The number of mapper nodes to use.
	 * @param reducers - The number of reducer nodes to use.
	 * @return
	 */
	<InterKey, InterVal, OutKey, OutVal> StatusTracker requestProcess(
			Mapper<InterKey, InterVal> mapper, 
			Reducer<InterKey, InterVal, OutKey, OutVal> reducer, 
			URI inputURL, URI outputURL, int mappers, int reducers);
	
	/**
	 * Requests that a certain 
	 * @param mapper - The mapper to use.
	 * @param reducer - The reducer to use.
	 * @param intermediateOutputStrategy - The output strategy to use for intermediate output.
	 * @param finalOutputStrategy - The output strategy to use for the final output.
	 * @param inputURL - The URI of the input files.
	 * @param outputURL - The URI where the output will be stored.
	 * @param mappers - The number of mapper nodes to use.
	 * @param reducers - The number of reducer nodes to use.
	 * @return
	 */
	<InterKey, InterVal, OutKey, OutVal> StatusTracker requestProcess(
			Mapper<InterKey, InterVal> mapper, 
			Reducer<InterKey, InterVal, OutKey, OutVal> reducer, 
			OutputStrategyFactory<InterKey, InterVal> intermediateOutputStrategy,
			OutputStrategyFactory<OutKey, OutVal> finalOutputStrategy,
			URI inputURL, URI outputURL, int mappers, int reducers);
	
	/**
	 * Adds a parser to the framework.
	 * @param clazz - The class.
	 * @param parser - The parser.
	 */
	<T> void addParser(Class<T> clazz, OutputParser<? extends T> parser);
	
}
