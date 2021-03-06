package mapreduce.parsers;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Scanner;

/**
 * Parser for ArrayList<T>
 * @author Admin
 *
 * @param <T>
 */
public class ArrayListParser<T> implements OutputParser<ArrayList<T>> {
	private OutputParser<T> parser;
	private String delimiter;
	
	public ArrayListParser(OutputParser<T> parser, String delimiter) {
		this.parser = parser;
		this.delimiter = delimiter;
	}
	

	@Override
	public ArrayList<T> parse(InputStream input) {
			return null;
	}

	@Override
	public void put(OutputStream output, ArrayList<T> obj) {
		PrintStream printer = new PrintStream(output);
		for(T e : obj) {
			parser.put(output, e);
			printer.print(delimiter);
		}
	}

}
