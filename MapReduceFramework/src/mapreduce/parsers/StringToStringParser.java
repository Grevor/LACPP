package mapreduce.parsers;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;

public class StringToStringParser implements OutputParser<String> {
	public static final StringToStringParser singleton = new StringToStringParser();
	
	private StringToStringParser() { }
	
	@Override
	public String parse(InputStream input) {
		throw new Error("");
	}

	@Override
	public void put(OutputStream output, String obj) {
		PrintStream p = new PrintStream(output);
		p.print(obj);
	}

}
