package mapreduce.mappers;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Scanner;

import mapreduce.Mapper;

public class EdgeToAdjacencyMapper extends Mapper<Long, Collection<Long>> {

	@Override
	public void map(String key, InputStream file) {
		Scanner scan = new Scanner(file);
		HashMap<Long, Collection<Long>> edges = new HashMap<>();
		
		while(scan.hasNextLine()) {
			String line = scan.nextLine();
			line = line.trim();
			line = line.substring(1, line.length() - 1);
			String[] nodes = line.split(",");
			long n1 = Long.parseLong(nodes[0]);
			long n2 = Long.parseLong(nodes[1]);
			
			addEdge(n1, n2, edges);
			addEdge(n2, n1, edges);
		}
		
		for(Entry<Long, Collection<Long>> e : edges.entrySet())
			emit(e.getKey(), e.getValue());
			
		scan.close();
	}

	private void addEdge(long n1, long n2, HashMap<Long, Collection<Long>> edges) {
		if(!edges.containsKey(n1)) {
			edges.put(n1, new ArrayList<Long>());
		}
		
		edges.get(n1).add(n2);
	}

}
