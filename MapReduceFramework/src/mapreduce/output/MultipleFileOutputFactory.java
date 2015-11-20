package mapreduce.output;

import java.net.URI;
import java.util.HashMap;

import mapreduce.OutputURICreator;
import mapreduce.parsers.KeyValueParser;
import mapreduce.parsers.ReadableParserCollection;

public class MultipleFileOutputFactory<Key,Value> implements OutputStrategyFactory<Key, Value> {
	private long maxFileSize;
	private HashMap<URI, OutputURICreator> creators = new HashMap<>();
	private Class<Key> keyClass;
	private Class<? extends Value> valueClass;
	private String keyValueDivider, entryDivider;
	
	public MultipleFileOutputFactory(long fileSize, String keyValueDivider, String entryDivider, 
			Class<Key> keyClass, Class<? extends Value> valueClass) {
		maxFileSize = fileSize;
		this.keyClass = keyClass;
		this.valueClass = valueClass;
		this.keyValueDivider = keyValueDivider;
		this.entryDivider = entryDivider;
	}

	@Override
	public OutputStrategy<Key, Value> create(URI output, ReadableParserCollection parsers) throws Exception {
		OutputURICreator uris = getCreator(output);
		return new MultipleFileOutput<>(uris, maxFileSize, 
				new KeyValueParser<>(parsers.getParser(keyClass), parsers.getParser(valueClass), 
						keyValueDivider, entryDivider));
	}

	private OutputURICreator getCreator(URI output) {
		if(!creators.containsKey(output))
			creators.put(output, new OutputURICreator(output));
		
		return creators.get(output);
		
	}

	@Override
	public void outputComplete(URI completedOutput) throws Exception { }

}
