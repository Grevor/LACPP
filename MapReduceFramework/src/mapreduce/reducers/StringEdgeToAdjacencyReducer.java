package mapreduce.reducers;

import java.util.ArrayList;
import java.util.Collection;

import mapreduce.Reducer;

public class StringEdgeToAdjacencyReducer extends Reducer<String, ArrayList<String>, String, ArrayList<String>> {
	@Override
	public void reduce(String key, Iterable<ArrayList<String>> values) {
		ArrayList<String> collection = new ArrayList<>(1000);
		for(Collection<String> c : values)
			collection.addAll(c);
		
		emit(key, collection);
	}
}
