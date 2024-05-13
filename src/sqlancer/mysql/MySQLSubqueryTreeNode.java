package sqlancer.mysql;

import sqlancer.mysql.ast.MySQLConstant;
import sqlancer.mysql.ast.MySQLExpression;
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
    private List<MySQLSubqueryTreeNode> whereComparisonSubquery;
    private MySQLSubqueryTreeNode fromSubquery;
    private MySQLSubqueryTreeNode ExistsSubquery;

    private MySQLSelect subquery;
    private MySQLGlobalState state;
    private Map<Integer, Map<MySQLColumn, MySQLConstant>> subqueryResult;

    private String createTableSQL;
    private String insertValuesSQL;

    private String createViewSQL;

    private MySQLExpression flattenedQuery;


    public MySQLSubqueryTreeNode(MySQLSelect subquery, MySQLGlobalState globalState) {
        this.subquery = subquery;
        this.state = globalState;
        this.whereComparisonSubquery = new ArrayList<>();
        this.subqueryResult = executeSubquery(subquery, globalState);
    }

    public void setFromNode(MySQLSubqueryTreeNode fromSubquery) {
         this.fromSubquery = fromSubquery;
    }

    public void addWhereSubquery(MySQLSubqueryTreeNode whereSubquery) {
        this.whereComparisonSubquery.add(whereSubquery);
    }

    public void addExistsSubquery(MySQLSubqueryTreeNode ExistsSubquery) {
        this.ExistsSubquery = ExistsSubquery;
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

    public List<MySQLSubqueryTreeNode> getWhereComparisonSubquery() {
        return whereComparisonSubquery;
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
        return createTableSQL == null ? "" : createTableSQL;
    }

    public String getInsertValuesSQL() {
        return insertValuesSQL == null ? "" : insertValuesSQL;
    }

    public MySQLSelect getSubquery() {
        return subquery;
    }

    public int getNodeNum() {
        return nodeNum;
    }

    public void setFlattenedQuery(MySQLExpression flattenedQuery) {
        this.flattenedQuery = flattenedQuery;
    }

    public MySQLExpression getFlattenedQuery() {
        return flattenedQuery;
    }
}
