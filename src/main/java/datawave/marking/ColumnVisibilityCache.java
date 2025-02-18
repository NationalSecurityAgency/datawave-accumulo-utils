package datawave.marking;

import java.util.Collections;
import java.util.Map;

import org.apache.accumulo.access.AccessExpression;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.hadoop.io.Text;

public class ColumnVisibilityCache {
    private static Map<ByteSequence,AccessExpression> cache = Collections.synchronizedMap(new LRUMap<>(256));
    
    /**
     * Validates the access expression in a key and returns a Text containing the valid expression.
     */
    public static Text validate(Key key) {
        var accessExpression = getExpression(key.getColumnVisibilityData());
        return new Text(accessExpression.getExpression());
    }
    
    static AccessExpression getExpression(ByteSequence bytes) {
        AccessExpression expression = cache.get(bytes);
        if (expression == null) {
            expression = AccessExpression.of(bytes.toArray());
            cache.put(new ArrayByteSequence(bytes), expression);
        }
        return expression;
    }
}
