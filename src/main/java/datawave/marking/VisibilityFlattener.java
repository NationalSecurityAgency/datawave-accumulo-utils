
package datawave.marking;

import static org.apache.accumulo.access.ParsedAccessExpression.ExpressionType.AND;
import static org.apache.accumulo.access.ParsedAccessExpression.ExpressionType.AUTHORIZATION;

import java.util.ArrayList;

import org.apache.accumulo.access.AccessExpression;
import org.apache.accumulo.access.ParsedAccessExpression;

public class VisibilityFlattener {
    public static AccessExpression flatten(AccessExpression root, boolean sort) {
        StringBuilder out = new StringBuilder();
        flatten(root.parse(), out, sort);
        return AccessExpression.of(out.toString());
    }

    private static int ordinal(ParsedAccessExpression accessExpression) {
        switch (accessExpression.getType()) {
            case EMPTY:
                return 0;
            case AUTHORIZATION:
                return 1;
            case OR:
                return 2;
            case AND:
                return 4;
            default:
                throw new IllegalArgumentException(accessExpression.getType().name());

        }
    }

    // This comparison logic is taken directly from ColumnVisibility.NodeComparator in order to have the same behavior.
    private static int compare(ParsedAccessExpression a, ParsedAccessExpression b) {
        int diff = ordinal(a) - ordinal(b);
        if (diff != 0) {
            return diff;
        }
        switch (a.getType()) {
            case EMPTY:
                return 0; // All empty nodes are the same
            case AUTHORIZATION:
                return AccessExpression.unquote(a.getExpression()).compareTo(AccessExpression.unquote(b.getExpression()));
            case AND:
                diff = a.getChildren().size() - b.getChildren().size();
                if (diff != 0) {
                    return diff;
                }
                for (int i = 0; i < a.getChildren().size(); i++) {
                    diff = compare(a.getChildren().get(i), b.getChildren().get(i));
                    if (diff != 0) {
                        return diff;
                    }
                }
        }

        return 0;
    }

    private static void flatten(ParsedAccessExpression root, StringBuilder out, boolean sort) {
        if (root.getType() == AUTHORIZATION)
            out.append(root.getExpression());
        else {
            String sep = "";
            var children = root.getChildren();
            if (sort) {
                children = new ArrayList<>(children);
                children.sort(VisibilityFlattener::compare);
            }
            for (var c : children) {
                out.append(sep);
                boolean parens = (c.getType() != AUTHORIZATION && root.getType() != c.getType());
                if (parens) {
                    out.append("(");
                }
                flatten(c, out, sort);
                if (parens) {
                    out.append(")");
                }
                sep = root.getType() == AND ? "&" : "|";
            }
        }
    }
}
