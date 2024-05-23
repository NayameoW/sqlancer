package sqlancer.mysql;

import sqlancer.mysql.ast.MySQLConstant;
import sqlancer.mysql.ast.MySQLSelect;
import sqlancer.mysql.MySQLSchema.MySQLColumn;
import sqlancer.mysql.MySQLSchema.MySQLDataType;

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
                createStatement.append(column.getName() + " " + getTypeFromConstant(column.getType()));
                first = false;
            }
        } else {
            return "CREATE TEMPORARY TABLE " + tableName + " (c0 INT, c1 INT, c2 INT);";
        }
        createStatement.append(");");
        return createStatement.toString();
    }

    private String getTypeFromConstant(MySQLDataType type) {
        switch (type) {
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
                throw new IllegalArgumentException("Unsupported type: " + type);
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

//    public String createWithStatement(MySQLSubqueryTreeNode node, String tableName) {
//        StringBuilder withStatement = new StringBuilder("WITH " + tableName + " AS (");
//        if (!node.getSubqueryResult().isEmpty()) {
//            withStatement.append("SELECT ");
//            Set<Entry<MySQLColumn, MySQLConstant>> entrySet = node.getSubqueryResult().values().iterator().next().entrySet();
//            boolean first = true;
//            for (Entry<MySQLColumn, MySQLConstant> entry : entrySet) {
//                if (!first) {
//                    withStatement.append(", ");
//                }
//                MySQLColumn column = entry.getKey();
//                if (entry.getValue().getType() != null) {
//                    withStatement.append(entry.getValue().getSQLRepresentation() + " AS " + column.getName());
//                } else {
//                    withStatement.append("NULL AS " + column.getName());
//                }
//                first = false;
//            }
//            withStatement.append(" UNION ALL ");
//            int rowCount = 0;
//            for (Map<MySQLColumn, MySQLConstant> row : node.getSubqueryResult().values()) {
//                if (rowCount > 0) {
//                    withStatement.append(" UNION ALL ");
//                }
//                withStatement.append("SELECT ");
//                first = true;
//                for (MySQLConstant value : row.values()) {
//                    if (!first) {
//                        withStatement.append(", ");
//                    }
//                    withStatement.append(value.getSQLRepresentation());
//                    first = false;
//                }
//                rowCount++;
//            }
//        } else {
//            return "";
//        }
//        withStatement.append(")");
//        return withStatement.toString();
//    }


}
