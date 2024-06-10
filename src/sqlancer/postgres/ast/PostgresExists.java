package sqlancer.postgres.ast;

public class PostgresExists implements PostgresExpression {

    private final PostgresExpression expr;

    public PostgresExists(PostgresExpression expr) {
        this.expr = expr;
    }

    public PostgresExpression getExpr() {
        return expr;
    }
}
