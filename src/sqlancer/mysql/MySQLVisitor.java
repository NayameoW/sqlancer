package sqlancer.mysql;

import sqlancer.mysql.ast.*;

public interface MySQLVisitor {

    void visit(MySQLTableReference ref);

    void visit(MySQLConstant constant);

    void visit(MySQLColumnReference column);

    void visit(MySQLUnaryPostfixOperation column);

    void visit(MySQLComputableFunction f);

    void visit(MySQLBinaryLogicalOperation op);

    void visit(MySQLSelect select);

    void visit(MySQLSubSelect subSelect);

    void visit(MySQLLimit limit);

    void visit(MySQLBinaryComparisonOperation op);

    void visit(MySQLCastOperation op);

    void visit(MySQLInOperation op);

    void visit(MySQLBinaryOperation op);

    void visit(MySQLOrderByTerm op);

    void visit(MySQLExists op);

    void visit(MySQLStringExpression op);

    void visit(MySQLBetweenOperation op);

    void visit(MySQLCollate collate);

    default void visit(MySQLExpression expr) {
        if (expr instanceof MySQLConstant) {
            visit((MySQLConstant) expr);
        } else if (expr instanceof MySQLColumnReference) {
            visit((MySQLColumnReference) expr);
        } else if (expr instanceof MySQLUnaryPostfixOperation) {
            visit((MySQLUnaryPostfixOperation) expr);
        } else if (expr instanceof MySQLComputableFunction) {
            visit((MySQLComputableFunction) expr);
        } else if (expr instanceof MySQLBinaryLogicalOperation) {
            visit((MySQLBinaryLogicalOperation) expr);
        } else if (expr instanceof MySQLSelect) {
            visit((MySQLSelect) expr);
        } else if (expr instanceof MySQLBinaryComparisonOperation) {
            visit((MySQLBinaryComparisonOperation) expr);
        } else if (expr instanceof MySQLCastOperation) {
            visit((MySQLCastOperation) expr);
        } else if (expr instanceof MySQLInOperation) {
            visit((MySQLInOperation) expr);
        } else if (expr instanceof MySQLBinaryOperation) {
            visit((MySQLBinaryOperation) expr);
        } else if (expr instanceof MySQLOrderByTerm) {
            visit((MySQLOrderByTerm) expr);
        } else if (expr instanceof MySQLExists) {
            visit((MySQLExists) expr);
        } else if (expr instanceof MySQLStringExpression) {
            visit((MySQLStringExpression) expr);
        } else if (expr instanceof MySQLBetweenOperation) {
            visit((MySQLBetweenOperation) expr);
        } else if (expr instanceof MySQLTableReference) {
            visit((MySQLTableReference) expr);
        } else if (expr instanceof MySQLCollate) {
            visit((MySQLCollate) expr);
        } else if (expr instanceof MySQLLimit) {
            visit((MySQLLimit) expr);
        } else {
            throw new AssertionError(expr);
        }
    }

    static String asString(MySQLExpression expr) {
        MySQLToStringVisitor visitor = new MySQLToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

    static String asExpectedValues(MySQLExpression expr) {
        MySQLExpectedValueVisitor visitor = new MySQLExpectedValueVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

}
