package mapreduce;

import java.util.ArrayList;

public class AggregateStatusTracker implements StatusTracker {
	ArrayList<StatusTracker> trackers = new ArrayList<>();
	
	public AggregateStatusTracker(StatusTracker... trackers) {
		for(StatusTracker t : trackers)
			this.trackers.add(t);
	}

	@Override
	public void waitUntilComplete() {
		for(StatusTracker t : trackers)
			t.waitUntilComplete();
	}

}
