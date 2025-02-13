package datawave.marking;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.apache.accumulo.access.AccessExpression.unquote;
import static org.apache.accumulo.access.ParsedAccessExpression.ExpressionType.AND;
import static org.apache.accumulo.access.ParsedAccessExpression.ExpressionType.AUTHORIZATION;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.apache.accumulo.access.AccessExpression;
import org.apache.accumulo.access.ParsedAccessExpression;
import org.apache.accumulo.core.data.ByteSequence;

/**
 * This is a cache that can be used per process to save flattened visibility calculations.
 *
 */
public class FlattenedVisibilityCache {
    private static Map<AccessExpression,byte[]> flattenedVisCache = Collections.synchronizedMap(new HashMap<>());
    
    /**
     * As part of normalizing access expression this class is used to sort and dedupe sub-expressions in a tree set.
     */
    public static class NormalizedExpression implements Comparable<NormalizedExpression> {
        public final String expression;
        public final ParsedAccessExpression.ExpressionType type;
        
        NormalizedExpression(String expression, ParsedAccessExpression.ExpressionType type) {
            this.expression = expression;
            this.type = type;
        }
        
        // determines the sort order of different kinds of subexpressions.
        private static int typeOrder(ParsedAccessExpression.ExpressionType type) {
            switch (type) {
                case AUTHORIZATION:
                    return 1;
                case OR:
                    return 2;
                case AND:
                    return 3;
                default:
                    throw new IllegalArgumentException("Unexpected type " + type);
            }
        }
        
        @Override
        public int compareTo(NormalizedExpression o) {
            // Changing this comparator would significantly change how expressions are normalized.
            int cmp = typeOrder(type) - typeOrder(o.type);
            if (cmp == 0) {
                if (type == AUTHORIZATION) {
                    // sort based on the unquoted and unescaped form of the authorization
                    cmp = unquote(expression).compareTo(unquote(o.expression));
                } else {
                    cmp = expression.compareTo(o.expression);
                }
                
            }
            return cmp;
        }
        
        @Override
        public boolean equals(Object o) {
            if (o instanceof NormalizedExpression) {
                return compareTo((NormalizedExpression) o) == 0;
            }
            return false;
        }
        
        @Override
        public int hashCode() {
            return expression.hashCode();
        }
    }
    
    /**
     * This method helps with the flattening aspect of normalization by recursing down as far as possible the parse tree in the case when the expression type is
     * the same. As long as the type is the same in the sub expression, keep using the same tree set.
     */
    public static void flatten(ParsedAccessExpression.ExpressionType parentType, ParsedAccessExpression parsed,
                    TreeSet<NormalizedExpression> normalizedExpressions) {
        if (parsed.getType() == parentType) {
            for (var child : parsed.getChildren()) {
                flatten(parentType, child, normalizedExpressions);
            }
        } else {
            // The type changed, so start again on the subexpression.
            normalizedExpressions.add(normalize(parsed));
        }
    }
    
    /**
     * <p>
     * For a given access expression this example will deduplicate, sort, flatten, and remove unneeded parentheses or quotes in the expressions. The following
     * list gives examples of what each normalization step does.
     *
     * <ul>
     * <li>As an example of flattening, the expression {@code A&(B&C)} flattens to {@code
     * A&B&C}.</li>
     * <li>As an example of sorting, the expression {@code (Z&Y)|(C&B)} sorts to {@code
     * (B&C)|(Y&Z)}</li>
     * <li>As an example of deduplication, the expression {@code X&Y&X} normalizes to {@code X&Y}</li>
     * <li>As an example of unneeded quotes, the expression {@code "ABC"&"XYZ"} normalizes to {@code ABC&XYZ}</li>
     * <li>As an example of unneeded parentheses, the expression {@code (((ABC)|(XYZ)))} normalizes to {@code ABC|XYZ}</li>
     * </ul>
     *
     * <p>
     * This algorithm attempts to have the same behavior as the one in the Accumulo 2.1 ColumnVisibility class. However the implementation is very different.
     * </p>
     */
    public static NormalizedExpression normalize(ParsedAccessExpression parsed) {
        if (parsed.getType() == AUTHORIZATION) {
            // If the authorization is quoted and it does not need to be quoted then the following two
            // lines will remove the unnecessary quoting.
            String unquoted = AccessExpression.unquote(parsed.getExpression());
            String quoted = AccessExpression.quote(unquoted);
            return new NormalizedExpression(quoted, parsed.getType());
        } else {
            // The tree set does the work of sorting and deduplicating sub expressions.
            TreeSet<NormalizedExpression> normalizedChildren = new TreeSet<>();
            for (var child : parsed.getChildren()) {
                flatten(parsed.getType(), child, normalizedChildren);
            }
            
            if (normalizedChildren.size() == 1) {
                return normalizedChildren.first();
            } else {
                String operator = parsed.getType() == AND ? "&" : "|";
                String sep = "";
                
                StringBuilder builder = new StringBuilder();
                
                for (var child : normalizedChildren) {
                    builder.append(sep);
                    if (child.type == AUTHORIZATION) {
                        builder.append(child.expression);
                    } else {
                        builder.append("(");
                        builder.append(child.expression);
                        builder.append(")");
                    }
                    sep = operator;
                }
                
                return new NormalizedExpression(builder.toString(), parsed.getType());
            }
        }
    }
    
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
            // TODO copied this normalize code from accumulo-access example. This used to call ColumnVisibility.flatten() and the accumulo-access example code
            // had the same behavior as flatten. The behavior differs from the code in VisibiityFlattern, need to reconcile this and converge on a single
            // algorithm..
            visBytes = normalize(accessExp.parse()).expression.getBytes(UTF_8);
            flattenedVisCache.put(accessExp, visBytes);
        }
        return visBytes;
    }
    
    public static byte[] flatten(ByteSequence bytes) {
        return flatten(ColumnVisibilityCache.getExpression(bytes));
    }
}
