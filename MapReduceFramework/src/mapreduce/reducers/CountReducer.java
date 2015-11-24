package mapreduce.reducers;

import mapreduce.Reducer;

public class CountReducer extends Reducer<String, Long, String, Long> {

	@Override
	public void reduce(String key, Iterable<Long> values) {
		long num = 0;
		for(Long l : values)
			num += l;
		
		emit(key, num / 6);
	}

}
