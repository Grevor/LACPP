package mapreduce.parsers;

public class ClassConverter {
	@SuppressWarnings("unchecked")
	public static <T> Class<T> convert(Class<?> clazz) {
		return (Class<T>) clazz;
	}
}
