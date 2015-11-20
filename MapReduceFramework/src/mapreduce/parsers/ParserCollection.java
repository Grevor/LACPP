package mapreduce.parsers;

import java.util.HashMap;

/**
 * Class representing a collection of {@link OutputParser}s.
 * The parsers can be retrieved based on their class.
 * @author Admin
 *
 */
public class ParserCollection implements ReadableParserCollection {
		private HashMap<Class<?>, OutputParser<?>> parsers = new HashMap<>();
		
		@Override
		public boolean has(Class<?> soughtParser) {
			return parsers.containsKey(soughtParser);
		}
		@Override
		@SuppressWarnings("unchecked")
		public <T> OutputParser<T> getParser(Class<? extends T> soughtParser) {
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
