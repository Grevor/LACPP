package mapreduce.output;

import java.net.URI;

import mapreduce.intermediate.InMemoryOutputStrategy;
import mapreduce.parsers.ReadableParserCollection;

/**
 * Factory for creating {@link InMemoryOutputStrategy}.
 * @author Admin
 *
 * @param <Key>
 * @param <Value>
 */
public class InMemoryOutputStrategyFactory<Key, Value> implements OutputStrategyFactory<Key, Value>{

	@Override
	public OutputStrategy<Key, Value> create(URI output, ReadableParserCollection parsers) throws Exception {
		return new InMemoryOutputStrategy<>();
	}

	@Override
	public void outputComplete(URI completedOutput) throws Exception { }

}
