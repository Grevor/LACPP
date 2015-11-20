package mapreduce.parsers;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Scanner;

/**
 * Parser for the String class.
 * @author Admin
 *
 */
public class StringParser implements OutputParser<String> {
	public static final StringParser singleton = new StringParser();
	
	private StringParser() { }
	
	@SuppressWarnings("resource")
	@Override
	public String parse(InputStream input) {
		return new Scanner(input).next();
	}

	@Override
	public void put(OutputStream output, String obj) {
		PrintStream p = new PrintStream(output);
		p.print(obj);
	}

}
