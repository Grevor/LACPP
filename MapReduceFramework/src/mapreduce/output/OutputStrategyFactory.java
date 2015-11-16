package mapreduce.output;

import java.net.URI;

import mapreduce.parsers.ReadableParserCollection;

public interface OutputStrategyFactory<Key, Value> {
	OutputStrategy<Key, Value> create(URI output, ReadableParserCollection parsers) throws Exception;
	void outputComplete(URI completedOutput) throws Exception;
}
