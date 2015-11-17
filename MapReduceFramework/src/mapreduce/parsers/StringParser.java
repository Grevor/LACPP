package mapreduce.parsers;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;

/**
 * Parser for the String class.
 * @author Admin
 *
 */
public class StringParser implements OutputParser<String> {
	public static final StringParser singleton = new StringParser();
	
	private StringParser() { }
	
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
