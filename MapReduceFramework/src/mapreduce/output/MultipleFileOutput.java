package mapreduce.output;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import mapreduce.MapReduceFileOutputStream;
import mapreduce.OutputURICreator;
import mapreduce.parsers.KeyValueParser;
import mapreduce.parsers.Pair;

public class MultipleFileOutput<Key, Value> implements OutputStrategy<Key, Value> {
	private OutputURICreator uriSupply;
	private MapReduceFileOutputStream output;
	private KeyValueParser<Key, Value> outputParser;
	private long maxFileSize;
	
	public MultipleFileOutput(OutputURICreator uriSupply, long maxFileSize, KeyValueParser<Key, Value> outputParser) throws FileNotFoundException {
		this.uriSupply = uriSupply;
		this.maxFileSize = maxFileSize;
		this.outputParser = outputParser;
		output = new MapReduceFileOutputStream(new File(uriSupply.getOutputURI()));
	}

	@Override
	public void emit(Key key, Value value) {
		try {
			long fileSize = output.getSize();
			if(fileSize > maxFileSize) {
				output.close();
				output = new MapReduceFileOutputStream(new File(uriSupply.getOutputURI()));
			}
			
			outputParser.put(output, new Pair<>(key, value));
			
		} catch(Exception e) {
			throw new Error("");
		}
	}

	@Override
	public Iterable<Key> getKeys() { throw new Error(""); }

	@Override
	public Iterable<Value> getValues(Key k) { throw new Error(""); }

	@Override
	public void outputComplete() throws IOException { output.close(); }

}
