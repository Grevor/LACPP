package mapreduce.output;

import java.io.OutputStream;
import java.util.concurrent.Semaphore;

import mapreduce.parsers.OutputParser;
import mapreduce.parsers.ParserCollection;

public class SingleFileOutput<OutKey, OutVal> implements OutputStrategy<OutKey, OutVal>{
	private OutputStream outputStream;
	private Semaphore sem;
	private ParserCollection parsers;
	private OutputParser<OutVal> valueParser;
	private OutputParser<OutKey> keyParser;
	
	public SingleFileOutput(OutputStream outputStream, ParserCollection parsers) {
		this.outputStream = outputStream;
		this.parsers = parsers;
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
		valueParser.put(outputStream, value);
		sem.release();
	}

}
