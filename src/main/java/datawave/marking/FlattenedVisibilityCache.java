package datawave.marking;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.access.AccessExpression;
import org.apache.accumulo.core.data.ByteSequence;

/**
 * This is a cache that can be used per process to save flattened visibility calculations.
 *
 */
public class FlattenedVisibilityCache {
    private static Map<AccessExpression,byte[]> flattenedVisCache = Collections.synchronizedMap(new HashMap<>());
    
    /**
     * Create a flattened expression, using the cache if possible
     *
     * @param accessExp
     *            the Access expression to flatten
     * @return the flattened expression
     */
    public static byte[] flatten(AccessExpression accessExp) {
        byte[] visBytes = flattenedVisCache.get(accessExp);
        if (visBytes == null) {
            visBytes = AccessExpression.of(accessExp.getExpression(), true).getExpression().getBytes(UTF_8);
            flattenedVisCache.put(accessExp, visBytes);
        }
        return visBytes;
    }
    
    public static byte[] flatten(ByteSequence bytes) {
        return flatten(ColumnVisibilityCache.getExpression(bytes));
    }
}
