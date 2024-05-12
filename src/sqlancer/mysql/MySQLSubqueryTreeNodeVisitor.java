package sqlancer.mysql;

import com.google.common.collect.Lists;
import sqlancer.mysql.ast.MySQLExpression;
import sqlancer.mysql.ast.MySQLSelect;
import sqlancer.mysql.MySQLSchema.MySQLTable;
import sqlancer.mysql.ast.MySQLTableReference;

public class MySQLSubqueryTreeNodeVisitor {

    private MySQLTemporaryTableManager manager = new MySQLTemporaryTableManager();
    private static int tableCount = 0;

    public void visit(MySQLSubqueryTreeNode node) {
        if (node == null) {
            return ;
        }

        node.setNodeNum(tableCount);
        // leaf node
        if (node.getFromSubquery() == null && node.getWhereSubqueries().isEmpty()) {
            String tableName = "tempTable" + tableCount++;
            String createTableSQL = manager.createTemporaryTableStatement(node, tableName);
            String insertValuesSQL = manager.generateInsertStatements(node, tableName);
            node.setCreateTableSQL(createTableSQL);
            node.setInsertValuesSQL(insertValuesSQL);
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
            visit(fromSubquery);
            String createTableSQL = fromSubquery.getCreateTableSQL();
            String insertValuesSQL = fromSubquery.getInsertValuesSQL();
            node.setCreateTableSQL(node.getCreateTableSQL() + createTableSQL);
            node.setInsertValuesSQL(node.getInsertValuesSQL() + insertValuesSQL);
        }


    }

    public static void clearTableCount() {
        tableCount = 0;
    }

    public void flattenNodeSubquery(MySQLSubqueryTreeNode node) {
        // leaf node: can be converted to a temporary table
        if (node.getFromSubquery() == null && node.getWhereSubqueries().isEmpty()) {
            MySQLTable tempTable = new MySQLTable("tempTable" + node.getNodeNum(), null, null, null);
            MySQLTableReference tableReference = new MySQLTableReference(tempTable);
            node.setFlattenedQuery(tableReference);
        }

        if (node.getFromSubquery() != null) {
            MySQLSelect flattenedQuery = new MySQLSelect();
            MySQLExpression flattenedNode = node.getFlattenedQuery();
            flattenedQuery.setFromList();
            node.setFlattenedQuery(flattenedQuery);
        }

        if (! node.getWhereSubqueries().isEmpty()) {
            MySQLExpression leftNodeExpression = node.getWhereSubqueries().get(0).getFlattenedQuery();
            MySQLExpression rightNodeExpression = node.getWhereSubqueries().get(1).getFlattenedQuery();
            String leftNodeString = MySQLVisitor.asString(leftNodeExpression);
            String rightNodeString = MySQLVisitor.asString(rightNodeExpression);

        }

    }

}
