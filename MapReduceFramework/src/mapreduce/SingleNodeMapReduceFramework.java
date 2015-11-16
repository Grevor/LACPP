package mapreduce;

import java.net.URI;

import mapreduce.output.InMemoryOutputStrategyFactory;
import mapreduce.output.OutputStrategyFactory;
import mapreduce.output.SingleFileOutputFactory;
import mapreduce.parsers.OutputParser;
import mapreduce.parsers.ParserCollection;

/**
 * MapReduce framework running as a multi-threaded program on the local machine.
 * @author Admin
 *
 */
public class SingleNodeMapReduceFramework implements MapReduceFramework {
	private ParserCollection parsers = new ParserCollection();
	private int workID = 0;

	@Override
	public <T> void addParser(Class<T> clazz, OutputParser<? extends T> parser) {
		parsers.addParser(clazz, parser);
	}

	@Override
	public <InterKey, InterVal, OutKey, OutVal> StatusTracker requestProcess(Mapper<InterKey, InterVal> mapper,
			Reducer<InterKey, InterVal, OutKey, OutVal> reducer, 
			URI inputURL, 
			URI outputURL, 
			int mappers,
			int reducers) {
		return requestProcess(mapper, reducer,
				new InMemoryOutputStrategyFactory<InterKey, InterVal>(), new SingleFileOutputFactory<OutKey, OutVal>(), 
				inputURL, outputURL, mappers, reducers);
	}

	@Override
	public <InterKey, InterVal, OutKey, OutVal> StatusTracker requestProcess(Mapper<InterKey, InterVal> mapper,
			Reducer<InterKey, InterVal, OutKey, OutVal> reducer,
			OutputStrategyFactory<InterKey, InterVal> intermediateOutputStrategy,
			OutputStrategyFactory<OutKey, OutVal> finalOutputStrategy, 
			URI inputURL, 
			URI outputURL, 
			int mappers,
			int reducers) {
		MasterThread<InterKey, InterVal, OutKey, OutVal> master = 
				new MasterThread<>(mapper, reducer, intermediateOutputStrategy, finalOutputStrategy, 
						inputURL, outputURL,Integer.toString(workID++), mappers, reducers, parsers);
		master.start();
		return master;
	}
}
