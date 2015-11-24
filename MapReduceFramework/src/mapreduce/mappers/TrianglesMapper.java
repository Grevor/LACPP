package mapreduce.mappers;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

import mapreduce.Mapper;
import mapreduce.parsers.Pair;

public class TrianglesMapper extends Mapper<String, Pair<String, ArrayList<String>>>{

	@Override
	public void map(String key, InputStream file) {
		Scanner scan = new Scanner(file);
		String line;
		
		while(scan.hasNext()) {
			line = scan.nextLine();
			Scanner entry = new Scanner(line);
			String emitKey = entry.next();
			
			ArrayList<String> adjacents = new ArrayList<>();
			while(entry.hasNext())
				adjacents.add(entry.next());
			
			adjacents = sort(adjacents);
			
			// --- Emit adjacency list for the node itself.
			emit(emitKey, new Pair<>(emitKey, adjacents));
			
			for(int i = 0; i < adjacents.size(); i++)
				//for(int j = 0; j < adjacents.size(); j++)
					emit(adjacents.get(i), new Pair<>(emitKey, adjacents));
		}
	}

	private ArrayList<String> sort(ArrayList<String> adjacents) {
		String[] arr = adjacents.toArray(new String[0]);
		Arrays.sort(arr);
		adjacents.clear();
		for(String s : arr)
			adjacents.add(s);
		return adjacents;
	}
}
