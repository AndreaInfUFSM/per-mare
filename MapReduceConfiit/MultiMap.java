
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author steffenel
 */
public class MultiMap<K, V> implements Serializable {

    HashMap<K, Collection<V>> map = new HashMap<K, Collection<V>>();

    public void MultiMap() {
    }

    public void add(K token, V value) {
        Collection<V> values = map.get(token);
        if (values == null) {
            values = new ArrayList<V>();
            map.put(token, values);
        }
        values.add(value);
    }

    public Set<K> getKeys() {
        return map.keySet();
    }

    public K getKey(int pos) {
        Set<K> keys = map.keySet();
        Iterator ikeys = keys.iterator();
        int count = 0;
        K key = null;
        if (pos < size()) {
            while (ikeys.hasNext() && count <= pos) {
                key = (K) ikeys.next();
                count++;
            }
            return key;
        } else {
            return null;
        }
    }

    public int size() {
        return map.size();
    }

    public Collection<V> get(K key) {
        return map.get(key);
    }

    public Iterator keyIterator(K key) {
        return map.get(key).iterator();
    }
}
