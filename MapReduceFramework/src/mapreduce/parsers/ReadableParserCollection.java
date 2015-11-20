package mapreduce.parsers;

/**
 * Interface to an immutable {@link ParserCollection}.
 * @author Admin
 *
 */
public interface ReadableParserCollection {

	/**
	 * Checks if this parser collection contain a parser for the specified class.
	 * @param soughtParser - The class of the parser.
	 * @return
	 * True if we indeed can find such a parser, else false.
	 */
	boolean has(Class<?> soughtParser);

	/**
	 * Gets the parser for a specific type.
	 * @param soughtParser - The class of the parser to get.
	 * @return
	 * The parser. If no parser exists, return null.
	 */
	<T> OutputParser<T> getParser(Class<? extends T> soughtParser);

}