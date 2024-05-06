package sqlancer.mysql;

import sqlancer.mysql.ast.MySQLConstant;
import sqlancer.mysql.ast.MySQLSelect;
import sqlancer.mysql.MySQLSchema.MySQLColumn;

import java.util.ArrayList;
import java.util.Map;
import java.util.List;

/**
 * Represents the tree structure of a query with subqueries
 */

public class MySQLSubqueryTreeNode {

    private int nodeNum;
    private List<MySQLSubqueryTreeNode> whereSubqueries;
    private MySQLSubqueryTreeNode fromSubquery;

    private MySQLSelect subquery;
    private MySQLGlobalState state;
    private Map<Integer, Map<MySQLColumn, MySQLConstant>> subqueryResult;

    private String createTableSQL;
    private String insertValuesSQL;

    public MySQLSubqueryTreeNode(MySQLSelect subquery, MySQLGlobalState globalState) {
        this.subquery = subquery;
        this.state = globalState;
        this.whereSubqueries = new ArrayList<>();
        this.subqueryResult = executeSubquery(subquery, globalState);
    }

    public void setFromNode(MySQLSubqueryTreeNode fromSubquery) {
         this.fromSubquery = fromSubquery;
    }

    public void addWhereSubquery(MySQLSubqueryTreeNode whereSubquery) {
        this.whereSubqueries.add(whereSubquery);
    }

    public Map<Integer, Map<MySQLColumn, MySQLConstant>> executeSubquery(MySQLSelect subquery, MySQLGlobalState state) {
        MySQLSubquerySupervisor supervisor = new MySQLSubquerySupervisor(subquery, state);
        return supervisor.getResult();
    }

    public Map<Integer, Map<MySQLColumn, MySQLConstant>> getSubqueryResult() {
        return subqueryResult;
    }

    public MySQLSubqueryTreeNode getFromSubquery() {
        return fromSubquery;
    }

    public List<MySQLSubqueryTreeNode> getWhereSubqueries() {
        return whereSubqueries;
    }

    public void setNodeNum(int nodeNum) {
        this.nodeNum = nodeNum;
    }

    public void setCreateTableSQL(String createTableSQL) {
        this.createTableSQL = createTableSQL;
    }

    public void setInsertValuesSQL(String insertValuesSQL) {
        this.insertValuesSQL = insertValuesSQL;
    }

    public String getCreateTableSQL() {
        return createTableSQL;
    }

    public String getInsertValuesSQL() {
        return insertValuesSQL;
    }
}
