package mapreduce.mappers;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Scanner;

import mapreduce.Mapper;

public class WordCountMapper extends Mapper<String, Long> {

	@Override
	public void map(String key, InputStream file) {
		HashMap<String, Long> counts = new HashMap<>();
		Scanner scan = new Scanner(file);
		scan.useDelimiter("[ ,.:;\r\n\t\"“”?!]"); // Fairly good delimiter pattern. (read: better than default)
		while(scan.hasNext()) {
			String word = scan.next().toLowerCase();
			if(word.equals(""))
				continue;
			
			if(!counts.containsKey(word))
				counts.put(word, 0L);
			
			counts.put(word, counts.get(word) + 1);
		}
		
		for(Entry<String, Long> e : counts.entrySet())
			emit(e.getKey(), e.getValue());
		
		scan.close();
	}

}
