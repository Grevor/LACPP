package mapreduce.parsers;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;

public class LongToStringParser implements OutputParser<Long> {
public static final LongToStringParser singleton = new LongToStringParser();
	
	private LongToStringParser() { }
	
	@Override
	public Long parse(InputStream input) {
		throw new Error("");
	}

	@Override
	public void put(OutputStream output, Long obj) {
		PrintStream p = new PrintStream(output);
		p.print(" " + obj.toString() + "\n");
	}

}
