package mapreduce.parsers;

/**
 * Small Utility class for general classes.
 * @author Admin
 *
 */
public class ClassConverter {
	@SuppressWarnings("unchecked")
	public static <T> Class<T> convert(Class<?> clazz) {
		return (Class<T>) clazz;
	}
}
