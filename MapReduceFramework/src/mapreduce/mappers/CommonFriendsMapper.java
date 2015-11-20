package mapreduce.mappers;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Scanner;

import mapreduce.Mapper;
import mapreduce.parsers.Pair;

public class CommonFriendsMapper extends Mapper<String, ArrayList<String>> {
//	private KeyValueParser<String, ArrayList<String>> parser = 
//			new KeyValueParser<>(StringParser.singleton, new ArrayListParser<>(StringParser.singleton, " "), 
//					" # ", "\n");

	@Override
	public void map(String key, InputStream file) {
		HashMap<String, ArrayList<String>> friendMappings = new HashMap<>();
		Scanner scan = new Scanner(file);
		while(scan.hasNextLine()) {
			Entry<String, ArrayList<String>> entry = parse(scan.nextLine());
			ArrayList<String> adjacent = entry.getValue();
			String commonFriend = entry.getKey();
			
			for(int i = 0; i < adjacent.size(); i++)
				for(int j = i + 1; j < adjacent.size(); j++)
					put(adjacent.get(i), adjacent.get(j), commonFriend, friendMappings);
		}
		
		for(Entry<String, ArrayList<String>> e : friendMappings.entrySet())
			emit(e.getKey(), e.getValue());
	}
	
	private Entry<String, ArrayList<String>> parse(String file) {
		Scanner scan = new Scanner(file);
		String key = scan.next();
		scan.skip(" # ");
		ArrayList<String> list = new ArrayList<>();
		while(scan.hasNext()) {
			list.add(scan.next());
		}
		return new Pair<>(key, list);
	}

	private void put(String first, String second, String friend, HashMap<String, ArrayList<String>> mappings) {
		String key = first.compareTo(second) > 0 
				? first + "_" + second 
				: second + "_" + first;
		
		if(!mappings.containsKey(key)) {
			mappings.put(key, new ArrayList<String>());
		}
		mappings.get(key).add(friend);
	}
}
