package mapreduce.mappers;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Scanner;

import mapreduce.Mapper;

public class StringEdgeToAdjacencyMapper extends Mapper<String, ArrayList<String>> {
	@Override
	public void map(String key, InputStream file) {
		Scanner scan = new Scanner(file);
		HashMap<String, ArrayList<String>> edges = new HashMap<>();
		
		while(scan.hasNextLine()) {
			String line = scan.nextLine();
			line = line.trim();
			line = line.substring(1, line.length() - 1);
			String[] nodes = line.split(",");
			String n1 = nodes[0].trim();
			String n2 = nodes[1].trim();
			
			addEdge(n1, n2, edges);
			addEdge(n2, n1, edges);
		}
		
		for(Entry<String, ArrayList<String>> e : edges.entrySet())
			emit(e.getKey(), e.getValue());
			
		scan.close();
	}

	private void addEdge(String n1, String n2, HashMap<String, ArrayList<String>> edges) {
		if(!edges.containsKey(n1)) {
			edges.put(n1, new ArrayList<String>());
		}
		
		edges.get(n1).add(n2);
	}
}
