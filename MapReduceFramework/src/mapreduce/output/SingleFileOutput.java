package mapreduce.output;

import java.io.OutputStream;
import java.util.concurrent.Semaphore;

public class SingleFileOutput<OutKey, OutVal> implements OutputStrategy<OutKey, OutVal>{
	OutputStream outputStream;
	Semaphore sem;
	
	@Override
	public void emit(OutKey key, OutVal value) {
		sem.acquireUninterruptibly();
		//write
	}

}
