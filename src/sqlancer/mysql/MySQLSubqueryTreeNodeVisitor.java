package sqlancer.mysql;

public class MySQLSubqueryTreeNodeVisitor {

    private MySQLTemporaryTableManager manager = new MySQLTemporaryTableManager();
    private int tableCount = 0;

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
}
