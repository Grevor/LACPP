package mapreduce.parsers;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Scanner;

/**
 * Parser for the long datatype.
 * @author Admin
 *
 */
public class LongParser implements OutputParser<Long>{
	public static final LongParser singleton = new LongParser();
	
	private LongParser() { }

	@SuppressWarnings("resource")
	@Override
	public Long parse(InputStream input) {
		Scanner scan = new Scanner(input);
		return scan.nextLong();
	}

	@Override
	public void put(OutputStream output, Long obj) {
		PrintStream printer = new PrintStream(output);
		printer.print(obj);
	}

}
