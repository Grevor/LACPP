package mapreduce.reducers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import mapreduce.Reducer;
import mapreduce.parsers.Pair;

public class TrianglesReducer extends Reducer<String, Pair<String, ArrayList<String>>, String, Long> {

	@Override
	public void reduce(String key, Iterable<Pair<String, ArrayList<String>>> values) {
		HashMap<String, ArrayList<String>> adjacency = new HashMap<>();
		ArrayList<String> currentNodeList = new ArrayList<>();
		
		for(Pair<String, ArrayList<String>> e : values) {
			if(e.getKey().equals(key))
				currentNodeList.addAll(e.getValue());
			else
				adjacency.put(e.getKey(), e.getValue());
		}
		
		long numTriangles = 0;
		
		for(Entry<String, ArrayList<String>> e : adjacency.entrySet()) {
			numTriangles += countTriangles(currentNodeList, e.getKey(), e.getValue());
		}
		
		emit("triangles", numTriangles);
	}

	private long countTriangles(ArrayList<String> currentNodeList, String key, ArrayList<String> value) {
		long num = 0;
		int i = 0, j = 0;
		while(i < currentNodeList.size() && j < value.size()) {
			String nodeTarget = currentNodeList.get(i);
			String other = value.get(j);
			int cmp = nodeTarget.compareTo(other);
			if(cmp == 0) {
				i++; j++;
				num++;
			} else if(cmp < 0) {
				i++;
			} else {
				j++;
			}
		}
		return num;
	}
}
