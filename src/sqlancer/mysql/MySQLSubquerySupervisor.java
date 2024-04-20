package sqlancer.mysql;

import sqlancer.SQLConnection;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mysql.ast.MySQLExpression;
import sqlancer.mysql.ast.MySQLSelect;
import sqlancer.common.query.Query;

public class MySQLSubquerySupervisor {

    private final MySQLSelect selectQuery;


    public MySQLSubquerySupervisor(MySQLSelect selectQuery) {
        this.selectQuery = selectQuery;
        initialize();
    }

    private void initialize() {

    }

    public void checkQuery() {
        String queryString = MySQLVisitor.asString(selectQuery);
        Query<SQLConnection> queryAdapter = new SQLQueryAdapter(queryString);
        

    }

}