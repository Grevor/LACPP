package mapreduce.reducers;

import java.util.ArrayList;
import java.util.Collection;

import mapreduce.Reducer;

public class EdgeToAdjacencyReducer extends Reducer<Long, Collection<Long>, Long, Collection<Long>>{

	@Override
	public void reduce(Long key, Iterable<Collection<Long>> values) {
		Collection<Long> collection = new ArrayList<>(1000);
		for(Collection<Long> c : values)
			collection.addAll(c);
		
		emit(key, collection);
	}

}
