package sqlancer.sqlite3.ast;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


public class SQLite3Subquery extends SQLite3Select {

    protected SQLite3Select selectQuery;
    protected String queryString;

    private Map<SQLite3Expression, String> columnAliasMapping;
    private int depth;

    public SQLite3Subquery() {
        super();
    }

    public SQLite3Subquery(SQLite3Select select, int depth) {
        this.selectQuery = select;
        this.depth = depth;
    }

    public SQLite3Subquery(String queryString) {
        this.queryString = queryString;
    }

    public SQLite3Select getSelectQuery() {
        return selectQuery;
    }

    public void setSelectQuery(SQLite3Select selectQuery) {
        this.selectQuery = selectQuery;
    }

    public Map<SQLite3Expression, String> getColumnAliasMapping() {
        return columnAliasMapping;
    }

    public void generateAliasesForColumns() {
        AtomicInteger index = new AtomicInteger(0);
        this.getFetchColumns().forEach(col -> {
            this.columnAliasMapping.put(col, "sc" + depth + index.getAndIncrement());
        });
    }
}
