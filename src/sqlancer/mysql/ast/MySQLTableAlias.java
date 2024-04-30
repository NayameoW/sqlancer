package sqlancer.mysql.ast;

import sqlancer.mysql.MySQLSchema.MySQLTable;

public class MySQLTableAlias implements MySQLExpression{

    private final MySQLTable table;

    public MySQLTableAlias(MySQLTable table) {
        this.table = table;
    }

    public String getTableName() {
        return table.getName();
    }
}
