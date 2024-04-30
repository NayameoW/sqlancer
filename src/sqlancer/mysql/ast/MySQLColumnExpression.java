package sqlancer.mysql.ast;

import sqlancer.mysql.MySQLSchema.MySQLColumn;

public class MySQLColumnExpression implements MySQLExpression {
    private final MySQLColumn column;
    private final String columnAlias;
    private final String tableAlias;

    public MySQLColumnExpression(MySQLColumn column, String columnAlias, String tableAlias) {
        this.column = column;
        this.columnAlias = columnAlias;
        this.tableAlias = tableAlias;
    }

    public MySQLColumn getColumn() {
        return column;
    }

    public String getColumnAlias() {
        return columnAlias;
    }

    public String getTableAlias() {
        return tableAlias;
    }

}
