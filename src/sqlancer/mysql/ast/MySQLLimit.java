package sqlancer.mysql.ast;

public class MySQLLimit implements MySQLExpression {

    private final int limit;

    public MySQLLimit(int limit) {
        this.limit = limit;
    }

    public int getLimit() {
        return limit;
    }

}
