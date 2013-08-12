/*
 *  UNIVERSITE PARIS 1 (PANTHEON SORBONNE)
 *  MIAGE - UFR 27
 */
package org.permare.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author kirsch
 */
public class MultiMapTest {
    MultiMap<String, String> map;
    String key, key2;
    ArrayList<String> values;
    
    public MultiMapTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
        map = new MultiMap<String, String>();
        key = "key";
        key2 = "token";
        
        values = new ArrayList<String>();
        values.add("1");
        values.add("2");
        values.add("3");
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of add method, of class MultiMap.
     */
    @Test
    public void testAdd() {
        System.out.println("add");
        this.map.add(key, values.get(0));
        assertEquals(1,this.map.size());
    }

    /**
     * Test of getKeys method, of class MultiMap.
     */
    @Test
    public void testGetKeys() {
        System.out.println("getKeys");
        this.map.add(key, values.get(0));
        this.map.add(key, values.get(1));
        Set result = this.map.getKeys();
        assertEquals(1, result.size());
        assertTrue(result.contains(key));
    }

    /**
     * Test of getKey method, of class MultiMap.
     */
    @Test
    public void testGetKey() {
        System.out.println("getKey(pos)");
        int pos = 1;
        this.map.add(key, values.get(0));
        this.map.add(key2, values.get(1));
        Object expResult = key2;
        Object result = map.getKey(pos);
        assertEquals(expResult, result);
    }

    /**
     * Test of size method, of class MultiMap.
     */
    @Test
    public void testSize() {
        System.out.println("size");
        this.map.add(key, values.get(0));
        this.map.add(key2, values.get(1));
        int expResult = 2;
        int result = map.size();
        assertEquals(expResult, result);
    }

    /**
     * Test of getValues method, of class MultiMap.
     */
    @Test
    public void testGetValues() {
        System.out.println("getValues");
        this.map.add(key, values.get(0));
        this.map.add(key, values.get(1));
        this.map.add(key, values.get(2));
        Collection result = map.getValues(key);
        assertTrue(result.containsAll(values));
        assertEquals(3, result.size());
    }

    /**
     * Test of keyIterator method, of class MultiMap.
     */
    @Test
    public void testKeyIterator() {
        System.out.println("keyIterator");
        this.map.add(key, values.get(0));
        this.map.add(key, values.get(1));
        this.map.add(key, values.get(2));
        int count = 0;
        
        Iterator result = map.keyIterator(key);
        assertFalse( ( result == null) );
        
        while (result.hasNext()) {
            count++;
            result.next();
        }
        assertEquals(3, count);
    }

    /**
     * Test of isEmpty method, of class MultiMap.
     */
    @Test
    public void testIsEmpty() {
        System.out.println("isEmpty");
        boolean result = map.isEmpty();
        assertTrue(result);
    }

    /**
     * Test of containsKey method, of class MultiMap.
     */
    @Test
    public void testContainsKey() {
        System.out.println("containsKey");
        this.map.add(key, values.get(0));
        boolean expResult = false;
        boolean result = map.containsKey(key2);
        assertEquals(expResult, result);
    }

    /**
     * Test of containsValue method, of class MultiMap.
     */
    @Test
    public void testContainsValue() {
        System.out.println("containsValue");
        this.map.add(key, values.get(0));
        boolean expResult = false;
        boolean result = map.containsValue(values.get(1));
        assertEquals(expResult, result);
    }

    /**
     * Test of get method, of class MultiMap.
     */
    @Test
    public void testGet() {
        System.out.println("get");
        this.map.add(key, values.get(0));
        this.map.add(key, values.get(1));
        
        Object result = map.get(key);
        assertEquals(values.get(0), result);
    }

    /**
     * Test of put method, of class MultiMap.
     */
    @Test
    public void testPut() {
        System.out.println("put");
        this.map.add(key, values.get(0));
        this.map.add(key, values.get(1));
        this.map.add(key, values.get(2));
        
        Collection<String> result = (Collection<String>) 
                this.map.put(key, key2);
        
        assertTrue(result.containsAll(values));
        
        result = this.map.getValues(key);
        assertTrue(result.contains(key2));
        assertEquals(1, result.size());
    }

    /**
     * Test of remove method, of class MultiMap.
     */
    @Test
    public void testRemove() {
        System.out.println("remove");
        this.map.add(key, values.get(0));
        this.map.add(key, values.get(1));
        this.map.add(key2, values.get(2));
        String result = map.remove(key);
        
        assertEquals(values.get(0), result);
    }

    /**
     * Test of putAll method, of class MultiMap.
     */
    @Test
    public void testPutAll_Map() {
        System.out.println("putAll");
        Map<String, String> m = new HashMap();
        m.put(key, values.get(0));
        m.put(key2, values.get(1));
        map.putAll(m);
        assertEquals(2, map.size());
    }

    /**
     * Test of putAll method, of class MultiMap.
     */
    @Test
    public void testPutAll_MultiMap() {
        System.out.println("putAll");
        MultiMap instance = new MultiMap();
        instance.add(key, values.get(0));
        instance.add(key2, values.get(1));
        map.add(key2, values.get(2));
        map.putAll(instance);
        assertEquals(2, map.size());
    }

    /**
     * Test of clear method, of class MultiMap.
     */
    @Test
    public void testClear() {
        System.out.println("clear");
        this.map.add(key, values.get(0));
        this.map.add(key, values.get(1));
        this.map.add(key2, values.get(2));
        map.clear();
        assertEquals(0, map.size());
    }

    /**
     * Test of keySet method, of class MultiMap.
     */
    @Test
    public void testKeySet() {
        System.out.println("keySet");
        this.map.add(key, values.get(0));
        this.map.add(key, values.get(1));
        this.map.add(key2, values.get(2));
        Set<String> expResult = new HashSet<String>();
        expResult.add(key);
        expResult.add(key2);
        Set<String> result = map.keySet();
        assertTrue(expResult.containsAll(result));
    }

    /**
     * Test of values method, of class MultiMap.
     */
    @Test
    public void testValues() {
        System.out.println("values");
        this.map.add(key, values.get(0));
        this.map.add(key, values.get(1));
        this.map.add(key, values.get(2));
        this.map.add(key2, values.get(0));
        this.map.add(key2, values.get(1));
        this.map.add(key2, values.get(2));
        Collection<Collection<String>> result = map.values();
        
        assertEquals(2,result.size());
        
        Iterator it = result.iterator();
        while (it.hasNext()) {
            Collection<String> c = (Collection<String>)it.next();
            assertTrue(c.containsAll(values));
        }
        
    }

    /**
     * Test of entrySet method, of class MultiMap.
     */
    @Test (expected = UnsupportedOperationException.class)
    public void testEntrySet() {
        System.out.println("entrySet");
        Set expResult = null;
        Set result = map.entrySet();
        assertEquals(expResult, result);
    }
}
