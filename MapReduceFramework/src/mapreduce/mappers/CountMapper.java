package mapreduce.mappers;

import java.io.InputStream;
import java.util.Scanner;

import mapreduce.Mapper;

public class CountMapper extends Mapper<String, Long> {

	@Override
	public void map(String key, InputStream file) {
		Scanner scan = new Scanner(file);
		while(scan.hasNext())
			emit(scan.next(), scan.nextLong());
	}

}
