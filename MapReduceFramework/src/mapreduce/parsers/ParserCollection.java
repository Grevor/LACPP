package mapreduce.parsers;

import java.util.HashMap;

public class ParserCollection {
		private HashMap<Class<?>, OutputParser<?>> parsers = new HashMap<>();
		
		/**
		 * Checks if this parser collection contain a parser for the specified class.
		 * @param soughtParser - The class of the parser.
		 * @return
		 * True if we indeed can find such a parser, else false.
		 */
		public boolean has(Class<?> soughtParser) {
			return parsers.containsKey(soughtParser);
		}
		/**
		 * Gets the parser for a specific type.
		 * @param soughtParser - The class of the parser to get.
		 * @return
		 * The parser. If no parser exists, return null.
		 */
		@SuppressWarnings("unchecked")
		public <T> OutputParser<T> getParser(Class<T> soughtParser) {
			return (OutputParser<T>)parsers.get(soughtParser);
		}
		/**
		 * Adds a parser to this collection.
		 * @param target - The target class of the parser.
		 * @param parser - The parser itself.
		 */
		public <T> void addParser(Class<T> target, OutputParser<? extends T> parser) {
			parsers.put(target, parser);
		}

}
