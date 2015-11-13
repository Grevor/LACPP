package mapreduce.reducers;

import mapreduce.Reducer;

public class WordCountReducer extends Reducer<String, Long, String, Long>{

	@Override
	public void reduce(String key, Iterable<Long> values) {
		long count = 0;
		
		for(long l : values)
			count += l;
		
		emit(key, count);
	}

}
