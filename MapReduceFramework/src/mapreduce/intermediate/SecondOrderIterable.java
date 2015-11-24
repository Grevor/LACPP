package mapreduce.intermediate;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Class representing an iterable consisting of other iterables.
 * @author Admin
 *
 * @param <T>
 */
public class SecondOrderIterable<T> implements Iterable<T>{
	private ArrayList<Iterable<T>> iterables = new ArrayList<>();
	
	public SecondOrderIterable(Iterable<Iterable<T>> iterables) {
		for(Iterable<T> i : iterables)
			this.iterables.add(i);
	}

	@Override
	public Iterator<T> iterator() {
		return new SOIter<>(iterables.iterator());
	}
	
	
	private static class SOIter<T> implements Iterator<T>{
		private Iterator<Iterable<T>> mainIterator;
		private Iterator<T> current;
		
		public SOIter(Iterator<Iterable<T>> iter) {
			mainIterator = iter;
			findNextCurrent();
		}
		
		@Override
		public void remove() {
			
		}

		@Override
		public boolean hasNext() {
			return current != null && current.hasNext();
		}

		@Override
		public T next() {
			if(hasNext()) {
				T e = current.next();
				findNextCurrent();
				return e;
			}
			throw new Error("");
		}

		private void findNextCurrent() {
			while(mainIterator.hasNext()) {
				current = mainIterator.next().iterator();
				if(current.hasNext())
					return;
			}
			
			if(!hasNext())
				current = null;
		}
	}
}
