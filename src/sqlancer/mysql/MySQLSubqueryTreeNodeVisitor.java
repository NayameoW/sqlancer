package sqlancer.mysql;

import sqlancer.mysql.ast.*;
import sqlancer.mysql.MySQLSchema.MySQLColumn;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;


public class MySQLSubqueryTreeNodeVisitor {

    private MySQLTemporaryTableManager manager = new MySQLTemporaryTableManager();
    private static int tableCount = 0;
    private List<String> tableNames = new ArrayList<>();
    private String tableString = "";
    private boolean useUUID = true;

    public void visit(MySQLSubqueryTreeNode node) {
        if (node == null) {
            return ;
        }
        node.setNodeNum(tableCount);
        // leaf node
        if (node.getFromSubquery() == null && node.getWhereComparisonSubquery().isEmpty()) {
            executeLeafNode(node);
//            String tableName = "tempTable" + tableCount++;
//            String createTableSQL = manager.createTemporaryTableStatement(node, tableName);
//            String insertValuesSQL = manager.generateInsertStatements(node, tableName);
//            node.setCreateTableSQL(createTableSQL);
//            node.setInsertValuesSQL(insertValuesSQL);
        } else {
            // visit children
            for (MySQLSubqueryTreeNode whereSubquery : node.getWhereComparisonSubquery()) {
                visit(whereSubquery);
                String createTableSQL = whereSubquery.getCreateTableSQL();
                String insertValuesSQL = whereSubquery.getInsertValuesSQL();
                node.setCreateTableSQL(node.getCreateTableSQL() + createTableSQL);
                node.setInsertValuesSQL(node.getInsertValuesSQL() + insertValuesSQL);
            }
            MySQLSubqueryTreeNode fromSubquery = node.getFromSubquery();
            if (fromSubquery != null) {
                visit(fromSubquery);
            }
//            String createTableSQL = fromSubquery.getCreateTableSQL();
//            String insertValuesSQL = fromSubquery.getInsertValuesSQL();
//            node.setCreateTableSQL(node.getCreateTableSQL() + createTableSQL);
//            node.setInsertValuesSQL(node.getInsertValuesSQL() + insertValuesSQL);
            executeInternalNode(node);
        }


    }


    /**
     * Execute the leaf node which does not contain subqueries
     * @param node: A leaf node
     */
    private void executeLeafNode(MySQLSubqueryTreeNode node) {
        String tableName = generateTableNameWithUUID();
        String createTableSQL = manager.createTemporaryTableStatement(node, tableName);
        String insertValuesSQL = manager.generateInsertStatements(node, tableName);
        node.setCreateTableSQL(createTableSQL);
        node.setInsertValuesSQL(insertValuesSQL);

        // todo: execution?

    }

    /**
     * Execute the node which is not a leaf node
     * @param node: A node that has children
     */
    private void executeInternalNode(MySQLSubqueryTreeNode node) {
        String tableName;
        if (useUUID) {
            tableName = generateTableNameWithUUID();
        } else {
            tableName = generateTableNameWithCount();
        }
        String sql = buildSQLFromChildren(node, tableName);
        this.tableString = tableString + sql;
    }

    public String getTableString() {
        return tableString;
    }

    private String buildSQLFromChildren(MySQLSubqueryTreeNode node, String tableName) {
        StringBuilder SQLBuilder = new StringBuilder();
//        String createTableSQL = node.getCreateTableSQL();
//        SQLBuilder.append(createTableSQL);
        String tableSQL = "";
        String fromTableName;
        String columnNames;
        String whereClause;
        MySQLSelect select = node.getSubquery();

        if (node.getFromSubquery() != null) {
            if (useUUID) {
                fromTableName = generateTableNameWithUUID();
            } else {
                fromTableName = generateTableNameWithCount();
            }
            tableSQL = generateTempTable(node.getFromSubquery(), fromTableName);
            columnNames = getColumnNames(select, fromTableName, false);
        } else {
            fromTableName = getTableNames(select);
            columnNames = getColumnNames(select, null, true);
        }

        if (! node.getWhereComparisonSubquery().isEmpty()) {
            String leftValue = getSubqueryValue(node.getWhereComparisonSubquery().get(0));
            String rightValue = getSubqueryValue(node.getWhereComparisonSubquery().get(1));
            if (!leftValue.isEmpty() && !rightValue.isEmpty()) {
                whereClause = "(" + leftValue + " >= " + rightValue + ")";
            } else {
                whereClause = "NULL";
            }

        } else {
            whereClause = "";
        }
        SQLBuilder.append(tableSQL);

        // build the flattened SQL
        boolean addFlattenedSQL = true;
        if (addFlattenedSQL) {
            SQLBuilder.append("SELECT ");
            SQLBuilder.append(columnNames);
            SQLBuilder.append(" FROM ");
            SQLBuilder.append(fromTableName);
            if (node.getSubquery().getWhereClause() != null) {
                SQLBuilder.append(" WHERE ");
                SQLBuilder.append(whereClause);
            }
            SQLBuilder.append(";");
        }


        return SQLBuilder.toString();
    }

    private String generateTableNameWithUUID() {
        String tableName = "tempTable_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        tableNames.add(tableName);
        return tableName;
    }

    private String generateTableNameWithCount() {
        return "tempTable_" + tableCount;
    }

    private String generateTempTable(MySQLSubqueryTreeNode node, String tableName) {
        String createTableSQL = manager.createTemporaryTableStatement(node, tableName);
        String insertValuesSQL = manager.generateInsertStatements(node, tableName);
        return createTableSQL + insertValuesSQL;
    }

    private String generateView() {
        return "CREATE VIEW";
    }

    private String generateTempTableUsingWITH(MySQLSubqueryTreeNode node, String tableName) {
        return "WITH";
    }

    private String getTableNames(MySQLSelect select) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < select.getFromList().size(); i++) {
            if (i != 0) {
                sb.append(",");
            }
            assert select.getFromList().get(i) instanceof MySQLTableReference;
            MySQLTableReference tableReference = (MySQLTableReference) select.getFromList().get(i);
            sb.append(tableReference.getTable().getName());
        }
        return sb.toString();
    }

    private String getColumnNames(MySQLSelect select, String tableName, boolean useOriginalFromTable) {
        StringBuilder sb = new StringBuilder();
        if (select.getFetchColumns() == null) {
            sb.append(" * ");
        } else {
            for (int i = 0; i < select.getFetchColumns().size(); i++) {
                if (i != 0) {
                    sb.append(",");
                }
                assert select.getFetchColumns().get(i) instanceof MySQLColumnReference;
                MySQLColumnReference columnReference = (MySQLColumnReference) select.getFetchColumns().get(i);
                if (useOriginalFromTable) {
                    sb.append(columnReference.getColumn().getTable().getName());
                    sb.append(".");
                    sb.append(columnReference.getColumn().getName());
                } else {
                    sb.append(tableName);
                    sb.append(".");
                    sb.append(columnReference.getColumn().getName());
                }
            }
        }

        return sb.toString();
    }

    private String getSubqueryValue(MySQLSubqueryTreeNode node) {
        Map<Integer, Map<MySQLColumn, MySQLConstant>> subqueryResult = node.getSubqueryResult();
        if (subqueryResult.isEmpty()) {
            return "";
        }
        Map<MySQLColumn, MySQLConstant> row = subqueryResult.values().iterator().next();
        assert row.size() == 1;
        MySQLConstant constant = row.values().iterator().next();
        return constant.getTextRepresentation();
    }

    public List<String> getTableNames() {
        return tableNames;
    }
}
