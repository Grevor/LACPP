package mapreduce.output;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.concurrent.Semaphore;

import mapreduce.parsers.OutputParser;
import mapreduce.parsers.ReadableParserCollection;

/**
 * Writes to a single file through an interface.
 * This strategy cannot be used as an intermediate output, as keys and values will not be saved in any way.
 * This class is thread-safe.
 * 
 * @author Admin
 *
 * @param <OutKey>
 * @param <OutVal>
 */
public class SingleFileOutput<OutKey, OutVal> implements OutputStrategy<OutKey, OutVal> {
	private OutputStream outputStream;
	private PrintStream outputPrinter;
	private Semaphore sem;
	private ReadableParserCollection parsers;
	private OutputParser<OutVal> valueParser;
	private OutputParser<OutKey> keyParser;
	private String keyValueSeparator, entrySeparator;
	
	public SingleFileOutput(OutputStream outputStream, ReadableParserCollection parsers, 
			String separator, String entrySeparator) {
		this.outputStream = outputStream;
		this.parsers = parsers;
		keyValueSeparator = separator;
		this.entrySeparator = entrySeparator;
		outputPrinter = new PrintStream(outputStream);
		sem = new Semaphore(1);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void emit(OutKey key, OutVal value) {
		sem.acquireUninterruptibly();
		if(valueParser == null) {
			valueParser = (OutputParser<OutVal>) parsers.getParser(value.getClass());
			keyParser = (OutputParser<OutKey>) parsers.getParser(key.getClass());
		}
		keyParser.put(outputStream, key);
		outputPrinter.print(keyValueSeparator);
		valueParser.put(outputStream, value);
		outputPrinter.print(entrySeparator);
		sem.release();
	}

	@Override
	public Iterable<OutKey> getKeys() { throw new Error(""); }
	@Override
	public Iterable<OutVal> getValues(OutKey k) { throw new Error(""); }
	@Override
	public void outputComplete() { }
	
	void close() throws IOException { outputStream.close(); }
}
