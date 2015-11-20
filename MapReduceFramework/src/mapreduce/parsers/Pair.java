package mapreduce.parsers;

public class Pair<K, V> implements java.util.Map.Entry<K, V> {
	private K key;
	private V val;
	
	public Pair(K k, V v) { key = k; val = v; }
	@Override
	public K getKey() { return key; }
	@Override
	public V getValue() { return val; }
	@Override
	public V setValue(V value) { V temp = val; val = value; return temp; }
}
