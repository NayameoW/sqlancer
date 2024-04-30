package sqlancer.mysql.ast;

import sqlancer.mysql.MySQLSchema.MySQLTable;
import sqlancer.mysql.MySQLSchema.MySQLColumn;

public class MySQLColumnReference implements MySQLExpression {

    private final MySQLColumn column;
    private final MySQLConstant value;
    private MySQLTable aliasTable;

    public MySQLColumnReference(MySQLColumn column, MySQLConstant value) {
        this.column = column;
        this.value = value;
    }

    public MySQLColumnReference(MySQLColumn column, MySQLConstant value, MySQLTable aliasTable) {
        this.column = column;
        this.value = value;
        this.aliasTable = aliasTable;
    }

    public static MySQLColumnReference create(MySQLColumn column, MySQLConstant value) {
        return new MySQLColumnReference(column, value);
    }

    public static MySQLColumnReference createAlias(MySQLColumn column, MySQLTable table) {
        return new MySQLColumnReference(column, null, table);
    }

    public MySQLColumn getColumn() {
        return column;
    }

    public MySQLTable getAliasTable() {
        return aliasTable;
    }

    public String getTableAliasName() {
        return aliasTable == null ? "" : aliasTable.getName();
    }

    public MySQLConstant getValue() {
        return value;
    }

    @Override
    public MySQLConstant getExpectedValue() {
        return value;
    }

}
