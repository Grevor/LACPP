package mapreduce.output;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Hashtable;

import mapreduce.MapReduceFileOutputStream;
import mapreduce.parsers.ReadableParserCollection;

/**
 * Factory that creates {@link SingleFileutput}.
 * @author Admin
 *
 * @param <Key>
 * @param <Value>
 */
public class SingleFileOutputFactory<Key, Value> implements OutputStrategyFactory<Key, Value> {
	private String keyValueSeparator = " ", entrySeparator = "\n", filename;
	private Hashtable<URI, SingleFileOutput<Key, Value>> openStreams = new Hashtable<>();

	public SingleFileOutputFactory() { this(" ", "\n", "output.txt"); }
	public SingleFileOutputFactory(String keyValueSeparator, String entrySeparator, String filename) {
		this.keyValueSeparator = keyValueSeparator;
		this.entrySeparator = entrySeparator;
		this.filename = filename;
	}
	
	@Override
	public OutputStrategy<Key, Value> create(URI output, ReadableParserCollection parsers) throws FileNotFoundException {
		OutputStrategy<Key, Value> outputStream = getStream(output, parsers);
		return outputStream;
	}

	private SingleFileOutput<Key, Value> getStream(URI output, ReadableParserCollection parsers) throws Error {
		OutputStream outputStream = null;
		if(openStreams.containsKey(output))
			return openStreams.get(output);
		
		try {
			URI actualOutputURI = output.resolve(filename);
			File actualOutputFile = new File(actualOutputURI);
			actualOutputFile.createNewFile();
			outputStream = new MapReduceFileOutputStream(actualOutputFile);
		} catch (IOException e) {
			throw new Error("Failed to create or open output file.");
		}
		SingleFileOutput<Key, Value> outputStrategy = new SingleFileOutput<>(outputStream, parsers, keyValueSeparator, entrySeparator);
		openStreams.put(output, outputStrategy);
		return outputStrategy;
	}

	@Override
	public void outputComplete(URI completedOutput) throws Exception {
		openStreams.remove(completedOutput).close();
	}

}
