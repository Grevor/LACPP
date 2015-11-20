package mapreduce.parsers;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.regex.Pattern;

/**
 * OutputParser which outputs key-value pairs by using multiple output parsers, 
 * and optionally adding something in the middle.
 * @author Admin
 *
 * @param <K>
 * @param <V>
 */
public class KeyValueParser<K,V> implements OutputParser<Entry<K, V>>{
	private OutputParser<K> keyParser;
	private OutputParser<V> valueParser;
	private Pattern keyValueDivider;
	private Pattern entryDivider;
	
	public KeyValueParser(OutputParser<K> key, OutputParser<V> value, String middlePattern, String entryDivider) {
		keyParser = key;
		valueParser = value;
		keyValueDivider = Pattern.compile(middlePattern);
		this.entryDivider = Pattern.compile(entryDivider);
	}

	@SuppressWarnings("resource")
	@Override
	public Entry<K, V> parse(InputStream input) {
		K key = keyParser.parse(input);
		Scanner scan = new Scanner(input);
		scan.next(keyValueDivider);
		V value = valueParser.parse(input);
		scan.next(entryDivider);
		return new Pair<>(key, value);
	}

	@Override
	public void put(OutputStream output, Entry<K, V> obj) {
		
		keyParser.put(output, obj.getKey());
		PrintStream printer = new PrintStream(output);
		printer.print(keyValueDivider.pattern());
		valueParser.put(output, obj.getValue());
		printer.print(entryDivider);
	}
}
