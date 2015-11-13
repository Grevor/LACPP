package mapreduce;

import java.net.URI;

import mapreduce.parsers.OutputParser;
import mapreduce.parsers.ParserCollection;

public class SingleNodeMapReduceFramework implements MapReduceFramework {
	private ParserCollection parsers = new ParserCollection();

	@Override
	public <T> void addParser(Class<T> clazz, OutputParser<? extends T> parser) {
		parsers.addParser(clazz, parser);
	}

	@Override
	public <InterKey, InterVal, OutKey, OutVal> StatusTracker requestProcess(Mapper<InterKey, InterVal> mapper,
			Reducer<InterKey, InterVal, OutKey, OutVal> reducer, URI inputURL, URI outputURL, int mappers,
			int reducers) {
		MasterThread<InterKey, InterVal, OutKey, OutVal> master = 
				new MasterThread<>(mapper, reducer, inputURL, outputURL, "0", mappers, reducers, parsers);
		master.start();
		return master;
	}

}
