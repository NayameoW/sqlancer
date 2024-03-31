package sqlancer.sqlite3.ast;

import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.sqlite3.SQLite3GlobalState;
import sqlancer.sqlite3.SQLite3Visitor;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column;

import java.util.Map;

public class SQLite3TableSubquery extends SQLite3Subquery {
    private String alias;

    public SQLite3TableSubquery(SQLite3Select select, int depth) {
        super(select, depth);
    }
    public SQLite3TableSubquery(String queryString) {
        super(queryString);
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public String getQueryString() {
        return SQLite3Visitor.asString(selectQuery) + " AS " + alias;
    }

}
