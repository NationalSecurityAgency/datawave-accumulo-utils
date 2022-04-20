package datawave.cache;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.cache.interceptor.SimpleKey;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Test the CollectionSafeKeyGenerator class
 */
public class CollectionSafeKeyGeneratorTest {
    
    @Test
    public void testListCopy() {
        ArrayList<Object> list = new ArrayList<>();
        list.add(new Object());
        Object copy = CollectionSafeKeyGenerator.copyIfCollectionParam(list);
        Assertions.assertNotSame(copy, list);
        Assertions.assertEquals(copy, list);
    }
    
    @Test
    public void testSetCopy() {
        HashSet<Object> set = new HashSet<>();
        set.add(new Object());
        Object copy = CollectionSafeKeyGenerator.copyIfCollectionParam(set);
        Assertions.assertNotSame(copy, set);
        Assertions.assertEquals(copy, set);
    }
    
    @Test
    public void testSortedSetCopy() {
        TreeSet<String> set = new TreeSet<>();
        set.add("Hello World");
        Object copy = CollectionSafeKeyGenerator.copyIfCollectionParam(set);
        Assertions.assertNotSame(copy, set);
        Assertions.assertEquals(copy, set);
    }
    
    @Test
    public void testMapCopy() {
        HashMap<Object,Object> map = new HashMap<>();
        map.put(new Object(), new Object());
        Object copy = CollectionSafeKeyGenerator.copyIfCollectionParam(map);
        Assertions.assertNotSame(copy, map);
        Assertions.assertEquals(copy, map);
    }
    
    @Test
    public void testSortedMapCopy() {
        TreeMap<String,Object> map = new TreeMap<>();
        map.put("Hello World", new Object());
        Object copy = CollectionSafeKeyGenerator.copyIfCollectionParam(map);
        Assertions.assertNotSame(copy, map);
        Assertions.assertEquals(copy, map);
    }
    
    @Test
    public void testEmptyKey() {
        Object key = CollectionSafeKeyGenerator.generateKey();
        Assertions.assertSame(key, SimpleKey.EMPTY);
    }
    
    @Test
    public void testNonCollectionSingleParam() {
        Object obj1 = new Object();
        Object key = CollectionSafeKeyGenerator.generateKey(obj1);
        Assertions.assertSame(key, obj1);
    }
    
    @Test
    public void testNonCollectionMultiParam() {
        Object obj1 = new Object();
        Object obj2 = new Object();
        Object key = CollectionSafeKeyGenerator.generateKey(obj1, obj2);
        Assertions.assertTrue(key instanceof SimpleKey);
        Assertions.assertEquals(new SimpleKey(obj1, obj2), key);
    }
    
    @Test
    public void testCollectionSingleParam() {
        ArrayList<Object> list = new ArrayList<>();
        list.add(new Object());
        Object key = CollectionSafeKeyGenerator.generateKey(list);
        Assertions.assertNotSame(key, list);
        Assertions.assertEquals(key, list);
    }
    
    @Test
    public void testCollectionMultiParam() throws IllegalAccessException, NoSuchFieldException {
        ArrayList<Object> list = new ArrayList<>();
        list.add(new Object());
        HashSet<Object> set = new HashSet<>();
        set.add(new Object());
        Object key = CollectionSafeKeyGenerator.generateKey(list, set);
        Assertions.assertTrue(key instanceof SimpleKey);
        Assertions.assertEquals(new SimpleKey(list, set), key);
        // extract the SimpleKey params and test those
        Field field = SimpleKey.class.getDeclaredField("params");
        field.setAccessible(true);
        Object[] params = (Object[]) field.get(key);
        Assertions.assertEquals(2, params.length);
        Assertions.assertNotSame(params[0], list);
        Assertions.assertEquals(params[0], list);
        Assertions.assertNotSame(params[1], set);
        Assertions.assertEquals(params[1], set);
    }
    
    @Test
    public void testMixedMultiParam() throws IllegalAccessException, NoSuchFieldException {
        ArrayList<Object> list = new ArrayList<>();
        list.add(new Object());
        Object obj1 = new Object();
        HashSet<Object> set = new HashSet<>();
        set.add(new Object());
        Object obj2 = new Object();
        Object key = CollectionSafeKeyGenerator.generateKey(list, obj1, set, obj2);
        Assertions.assertTrue(key instanceof SimpleKey);
        Assertions.assertEquals(new SimpleKey(list, obj1, set, obj2), key);
        // extract the SimpleKey params and test those
        Field field = SimpleKey.class.getDeclaredField("params");
        field.setAccessible(true);
        Object[] params = (Object[]) field.get(key);
        Assertions.assertEquals(4, params.length);
        Assertions.assertNotSame(params[0], list);
        Assertions.assertEquals(params[0], list);
        Assertions.assertSame(params[1], obj1);
        Assertions.assertNotSame(params[2], set);
        Assertions.assertEquals(params[2], set);
        Assertions.assertSame(params[3], obj2);
    }
    
}
