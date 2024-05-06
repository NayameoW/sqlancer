package sqlancer.mysql;

import sqlancer.mysql.ast.MySQLConstant;
import sqlancer.mysql.ast.MySQLSelect;
import sqlancer.mysql.MySQLSchema.MySQLColumn;

import java.util.Set;
import java.util.Map;
import java.util.Map.Entry;

public class MySQLTemporaryTableManager {

    public String createTemporaryTableStatement(MySQLSubqueryTreeNode node, String tableName) {
        StringBuilder createStatement = new StringBuilder("CREATE TEMPORARY TABLE " + tableName + " (");
        if (!node.getSubqueryResult().isEmpty()) {
            Set<Entry<MySQLColumn, MySQLConstant>> entrySet = node.getSubqueryResult().values().iterator().next().entrySet();
            boolean first = true;
            for (Entry<MySQLColumn, MySQLConstant> entry : entrySet) {
                if (!first) {
                    createStatement.append(", ");
                }
                MySQLColumn column = entry.getKey();
                createStatement.append(column.getName() + " " + entry.getValue().getType());
                first = false;
            }
        } else {
            return "";
        }
        createStatement.append(");");
        return createStatement.toString();
    }

    private String getTypeFromConstant(MySQLConstant constant) {
        switch (constant.getType()) {
            case INT:
                return "INT";
            case VARCHAR:
                return "VARCHAR(255)";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE PRECISION";
            case DECIMAL:
                return "DECIMAL";
            default:
                throw new IllegalArgumentException("Unsupported type: " + constant.getType());
        }
    }

    public String generateInsertStatements(MySQLSubqueryTreeNode node, String tableName) {
        StringBuilder insertStatement = new StringBuilder("INSERT INTO " + tableName + " VALUES ");
        int rowCount = 0;
        if (node.getSubqueryResult().isEmpty()) {
            return "";
        }
        for (Map<MySQLColumn, MySQLConstant> row : node.getSubqueryResult().values()) {
            if (rowCount > 0) {
                insertStatement.append(", ");
            }
            insertStatement.append("(");
            boolean first = true;
            for (MySQLConstant value : row.values()) {
                if (!first) {
                    insertStatement.append(", ");
                }
                insertStatement.append(convertConstantToSQLValue(value));
                first = false;
            }
            insertStatement.append(")");
            rowCount++;
        }
        insertStatement.append(";");
        return insertStatement.toString();
    }

    private String convertConstantToSQLValue(MySQLConstant constant) {
        if (constant.isNull()) {
            return "NULL";
        }
        switch (constant.getType()) {
            case VARCHAR:
                return "'" + constant.toString().replace("'", "''") + "'";
            default:
                return constant.toString();
        }
    }
}
