package sqlancer.common.oracle;

import sqlancer.Main.StateLogger;
import sqlancer.MainOptions;
import sqlancer.SQLConnection;
import sqlancer.SQLGlobalState;
import sqlancer.SQLancerDBConnection;
import sqlancer.common.query.Query;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.schema.AbstractRowValue;
import sqlancer.common.schema.AbstractTables;
import sqlancer.sqlite3.schema.SQLite3Schema;

import java.util.ArrayList;
import java.util.List;

public abstract class SubBase<S extends SQLGlobalState<?, ?>, R extends AbstractRowValue<?, ?, ?>, E, C extends SQLancerDBConnection>
        implements TestOracle<S> {

    protected final S state;
    protected final ExpectedErrors errors = new ExpectedErrors();
    protected final StateLogger logger;
    protected final MainOptions options;
    protected final SQLConnection con;

    protected R targetRows;
    protected final int subqueryDepth = 3;
    protected String subQueryString;
    protected String flattenedQueryString;
    protected final List<E> rectifiedPredicates = new ArrayList<>();
    private List<SQLite3Schema.SQLite3Column> fetchColumns;
    protected List<E> pivotRowExpression = new ArrayList<>();



    protected SubBase(S state) {
        this.state = state;
        this.con = state.getConnection();
        this.logger = state.getLogger();
        this.options = state.getOptions();
    }



}