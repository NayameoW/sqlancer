package sqlancer.mysql;

import com.google.common.collect.Lists;
import sqlancer.mysql.ast.MySQLExpression;
import sqlancer.mysql.ast.MySQLSelect;
import sqlancer.mysql.MySQLSchema.MySQLTable;
import sqlancer.mysql.ast.MySQLTableReference;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class MySQLSubqueryTreeNodeVisitor {

    private MySQLTemporaryTableManager manager = new MySQLTemporaryTableManager();
    private static int tableCount = 0;
    private List<String> tableNames = new ArrayList<>();

    public void visit(MySQLSubqueryTreeNode node) {
        if (node == null) {
            return ;
        }

        node.setNodeNum(tableCount);
        // leaf node
        if (node.getFromSubquery() == null && node.getWhereSubqueries().isEmpty()) {
            executeLeafNode(node);
//            String tableName = "tempTable" + tableCount++;
//            String createTableSQL = manager.createTemporaryTableStatement(node, tableName);
//            String insertValuesSQL = manager.generateInsertStatements(node, tableName);
//            node.setCreateTableSQL(createTableSQL);
//            node.setInsertValuesSQL(insertValuesSQL);
        } else {
            // visit children
            for (MySQLSubqueryTreeNode whereSubquery : node.getWhereSubqueries()) {
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
            String createTableSQL = fromSubquery.getCreateTableSQL();
            String insertValuesSQL = fromSubquery.getInsertValuesSQL();
            node.setCreateTableSQL(node.getCreateTableSQL() + createTableSQL);
            node.setInsertValuesSQL(node.getInsertValuesSQL() + insertValuesSQL);
        }


    }


    /**
     * Execute the leaf node which does not contain subqueries
     * @param node
     */
    private void executeLeafNode(MySQLSubqueryTreeNode node) {
        String tableName = generateTableNameWithUUID();
        tableNames.add(tableName);
        String createTableSQL = manager.createTemporaryTableStatement(node, tableName);
        String insertValuesSQL = manager.generateInsertStatements(node, tableName);
        node.setCreateTableSQL(createTableSQL);
        node.setInsertValuesSQL(insertValuesSQL);

        // execution?

    }

    private void executeInternalNode(MySQLSubqueryTreeNode node) {
        String tableName = generateTableNameWithUUID();
        tableNames.add(tableName);
        String sql = buildSQLFromChildren(node, tableName);
        node.setCreateTableSQL(sql);
    }

    private String buildSQLFromChildren(MySQLSubqueryTreeNode node, String tableName) {
        StringBuilder SQLBuilder = new StringBuilder();
        String createTableSQL = node.getCreateTableSQL();
        return SQLBuilder.toString();
    }

    private String generateTableNameWithUUID() {
        return "tempTable_" + UUID.randomUUID().toString().replace("-", "");
    }

    public static void clearTableCount() {
        tableCount = 0;
    }

    private void flattenNodeSubquery(MySQLSubqueryTreeNode node) {
        // leaf node: can be converted to a temporary table
        if (node.getFromSubquery() == null && node.getWhereSubqueries().isEmpty()) {
            MySQLTable tempTable = new MySQLTable("tempTable" + node.getNodeNum(), null, null, null);
            MySQLTableReference tableReference = new MySQLTableReference(tempTable);
            node.setFlattenedQuery(tableReference);
        }

        if (node.getFromSubquery() != null) {
            MySQLSelect flattenedQuery = new MySQLSelect();
            MySQLExpression flattenedNode = node.getFlattenedQuery();
            flattenedQuery.setFromList(Lists.asList(flattenedNode));
            node.setFlattenedQuery(flattenedQuery);
        }

    }

}
