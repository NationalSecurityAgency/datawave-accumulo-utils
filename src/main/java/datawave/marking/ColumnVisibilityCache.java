package datawave.marking;

import java.util.Collections;
import java.util.Map;

import org.apache.accumulo.access.AccessExpression;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.collections4.map.LRUMap;

public class ColumnVisibilityCache {
    private static Map<ByteSequence,AccessExpression> cache = Collections.synchronizedMap(new LRUMap<>(256));
    
    public static ColumnVisibility get(ByteSequence bytes) {
        return new ColumnVisibility(getExpression(bytes).getExpression());
    }
    
    static AccessExpression getExpression(ByteSequence bytes) {
        AccessExpression expression = cache.get(bytes);
        if (expression == null) {
            expression = AccessExpression.of(bytes.toArray());
            cache.put(bytes, expression);
        }
        return expression;
    }
}
