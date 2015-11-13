package mapreduce.threads;

import java.io.File;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.Semaphore;

import mapreduce.MapReduceFileInputStream;
import mapreduce.Mapper;
import mapreduce.WorkScheduler;

public class MapperThread<OutKey, OutVal> extends WorkPoolThread<URI> {
	private static final String desc = "Mapper Thread";
	private URI output;
	private Mapper<OutKey, OutVal> mapper;
	
	public MapperThread(Mapper<OutKey, OutVal> mapper,
			Semaphore reporter, URI output, 
			WorkScheduler<URI> scheduler, int threadIndex) 
	
	{
		super(reporter, scheduler, threadIndex, desc);
		this.output = output;
		this.mapper = mapper;
	}

	@Override
	public void run() {
		ArrayList<URI> outputs = new ArrayList<>();
		while(true) {
			URI input = tryGetWork();
			if(input == null)
				break;
			
			String id = input.getPath().toString();
			URI outputURI = getOutputURI(input);
			
			try {
				MapReduceFileInputStream dataStream = new MapReduceFileInputStream(new File(input));
				mapper.map(id, dataStream);
			} catch (Exception e) {
				System.err.println("the input '" + id + "' failed. Ignoring...");
			}
		}
		
		reportCompletion();
	}
	
	private URI getOutputURI(URI input) {
		return Paths.get(output).resolve(Paths.get(input).getFileName()).toUri();
	}
}
