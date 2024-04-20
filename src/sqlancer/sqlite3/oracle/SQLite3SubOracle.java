package sqlancer.sqlite3.oracle;

import sqlancer.*;
import sqlancer.common.oracle.SubBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.Query;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.sqlite3.SQLite3Errors;
import sqlancer.sqlite3.SQLite3GlobalState;
import sqlancer.sqlite3.SQLite3Visitor;
import sqlancer.sqlite3.ast.*;
import sqlancer.sqlite3.gen.SQLite3Common;
import sqlancer.sqlite3.gen.SQLite3ExpressionGenerator;
import sqlancer.sqlite3.schema.SQLite3Schema;
import sqlancer.StateToReproduce.OracleRunReproductionState;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3RowValue;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Table;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Tables;
import sqlancer.sqlite3.ast.SQLite3Expression.Sqlite3BinaryOperation;
import sqlancer.sqlite3.ast.SQLite3Expression.Sqlite3BinaryOperation.BinaryOperator;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3ColumnName;


import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class SQLite3SubOracle extends SubBase<SQLite3GlobalState, SQLite3RowValue, SQLite3Expression, SQLConnection> implements TestOracle<SQLite3GlobalState> {

    private final SQLite3Schema s;
    private SQLite3ExpressionGenerator gen;
    private Reproducer<SQLite3GlobalState> reproducer;
    private OracleRunReproductionState localState;
    private List<SQLite3Column> fetchColumns;


    public SQLite3SubOracle(SQLite3GlobalState globalState) {
        super(globalState);
        this.s = globalState.getSchema();
        SQLite3Errors.addExpectedExpressionErrors(errors);
        SQLite3Errors.addMatchQueryErrors(errors);
        SQLite3Errors.addQueryErrors(errors);
        errors.add("misuse of aggregate");
        errors.add("misuse of window function");
        errors.add("second argument to nth_value must be a positive integer");
        errors.add("no such table");
        errors.add("no query solution");
        errors.add("unable to use function MATCH in the requested context");
    }

    @Override
    public Reproducer<SQLite3GlobalState> getLastReproducer() {
        return reproducer;
    }

    @Override
    public String getLastQueryString() {
        return subQueryString;
    }


    @Override
    public final void check() throws Exception{
        int depth = 0;
        int rowCount = 3;
        SQLite3Tables randomFromTables = state.getSchema().getRandomTableNonEmptyTables();
        List<SQLite3Table> tables = randomFromTables.getTables();
        List<SQLite3Column> columns = randomFromTables.getColumns();
        targetRow = randomFromTables.getRandomRowValue(state.getConnection());
        targetRows = randomFromTables.getRandomRowValues(state.getConnection(), rowCount); // todo

        List<SQLite3Column> columnsWithoutRowid = columns.stream()
                .filter(c -> !SQLite3Schema.ROWID_STRINGS.contains(c.getName())).collect(Collectors.toList());
        fetchColumns = Randomly.nonEmptySubset(columnsWithoutRowid, 1);
        fetchColExpression = getColExpressions(false, fetchColumns, false, "");
        pivotRowExpressionWithoutTable = getColExpressions(false, fetchColumns, false, "");

//        AtomicInteger idx = new AtomicInteger();
//        List<SQLite3Column> currentColumns = fetchColumns.stream().map(column -> new SQLite3Column(
//                        column.getName() + idx.getAndIncrement(),
//                        column.getType(),
//                        column.isInteger(),
//                        column.isPrimaryKey(),
//                        column.getCollateSequence()
//                )).collect(Collectors.toList());
        List<SQLite3RowValue> currentRows = new ArrayList<>(targetRows);
//        Query<SQLConnection> testSelectStatement = getQuery(fetchColumns, currentRows, tables, true);
        Query<SQLConnection> innerSelectStatement = getQuery(fetchColumns, currentRows, tables, false, "");
        Query<SQLConnection> outerSelectStatement = innerSelectStatement;
        flattenedQueries.add(innerSelectStatement);

        SQLite3Select selectSubquery = getSelectStatement(fetchColumns, currentRows, tables, false, "");
        SQLite3Subquery subquery = new SQLite3Subquery(selectSubquery, 0);
        subquery.generateAliasesForColumns();

//        SQLite3Select selectSubquery2 = getSelectStatement(fetchColumns, currentRows, tables, false, "");
//        selectSubquery2.setFromTables(Arrays.asList(selectSubquery1));

        // generate subqueries
        String innerString = innerSelectStatement.getQueryString();
        String outerString = outerSelectStatement.getQueryString();
        while (depth < subqueryDepth) {
            String alias = "st" + depth;
            fetchColExpression = getColExpressions(false, fetchColumns, true, alias);
            targetRows = Randomly.nonEmptySubset(targetRows);
            outerSelectStatement = getQuery(fetchColumns, targetRows, tables, true, alias);
            innerString = innerSelectStatement.getQueryString();
            outerString = wrapSubQueryString(innerString, outerSelectStatement.getQueryString(), alias);
            outerSelectStatement = new SQLQueryAdapter(outerString, errors);
            innerSelectStatement = outerSelectStatement;
            flattenedQueries.add(outerSelectStatement);
//            outerSelectStatement = wrapSubQuery(innerSelectStatement, outerSelectStatement, alias);
//            innerSelectStatement = outerSelectStatement;
            depth ++;
        }

        if (state.getOptions().logEachSelect()) {
            logger.writeCurrent(outerSelectStatement.getQueryString());
        }

        // check results
//        SQLQueryAdapter subqueryAdapter = new SQLQueryAdapter(testSelectStatement.getQueryString(), errors);
        SQLQueryAdapter subqueryAdapter = new SQLQueryAdapter(outerSelectStatement.getQueryString(), errors);
//        SQLancerResultSet subqueryResultSet = subqueryAdapter.executeAndGet(state);
        List<SQLQueryAdapter> flattenedQueryAdapters = flattenedQueries.stream()
                .map(query -> new SQLQueryAdapter(query.getQueryString(), errors))
                .collect(Collectors.toList());
        try (SQLancerResultSet subqueryResultSet = subqueryAdapter.executeAndGet(state)) {
            int count = 0;
            if (subqueryResultSet == null) {
//                System.out.println("no results");
                throw new IgnoreMeException();
            } else {
//                if (! subqueryResultSet.next()) {
//                    System.out.println("no results");
//                }
                while (subqueryResultSet.next()) {
                    count++;
                }
            }
//            System.out.println(count);
        }
        // reset all columns to not use alias
        for (SQLite3Column c: columnsWithoutRowid) {
            c.setUseAlias(false);
        }
    }

    public void checkScalarSubquery() {

    }

    public Query<SQLConnection> getQuery(List<SQLite3Column> columnsWithoutRowid, List<SQLite3RowValue> rows, List<SQLite3Table> tables, boolean useAlias, String tableName) throws SQLException {
        assert !s.getDatabaseTables().isEmpty();
        localState = state.getState().getLocalState();
        assert localState != null;

        SQLite3Select selectStatement = new SQLite3Select();
//        selectStatement.setSelectType(Randomly.fromOptions(SQLite3Select.SelectType.values()));
        selectStatement.setFromList(SQLite3Common.getTableRefs(tables, state.getSchema()));

        List<SQLite3Table> allTables = new ArrayList<>();
        allTables.addAll(tables);
        selectStatement.setFetchColumns(fetchColExpression);

        if (useAlias) {
            for (SQLite3Column c: columnsWithoutRowid) {
                c.setUseAlias(true);
                c.setAlias(tableName);
            }
        }
        // concatenate all rows into a single WHERE clause
        List<SQLite3Expression> wherePredicates = new ArrayList<>();
        for (SQLite3RowValue row : rows) {
            SQLite3Expression predicate = generateRectifiedExpression(columnsWithoutRowid, row, false);
            wherePredicates.add(predicate);
        }
        SQLite3Expression whereClause = combinePredicates(wherePredicates);
        selectStatement.setWhereClause(whereClause);
//        List<SQLite3Expression> groupByClause = generateGroupByClause(columnsWithoutRowid, targetRows,
//                allTablesContainOneRow);
//        selectStatement.setGroupByClause(groupByClause);
        return new SQLQueryAdapter(SQLite3Visitor.asString(selectStatement), errors);
    }

    public SQLite3Select getSelectStatement(List<SQLite3Column> columnsWithoutRowid, List<SQLite3RowValue> rows, List<SQLite3Table> tables, boolean useAlias, String tableName) throws SQLException {
        assert !s.getDatabaseTables().isEmpty();
        localState = state.getState().getLocalState();
        assert localState != null;

        SQLite3Select selectStatement = new SQLite3Select();
//        selectStatement.setSelectType(Randomly.fromOptions(SQLite3Select.SelectType.values()));
        selectStatement.setFromList(SQLite3Common.getTableRefs(tables, state.getSchema()));

        List<SQLite3Table> allTables = new ArrayList<>();
        allTables.addAll(tables);
        selectStatement.setFetchColumns(fetchColExpression);

        if (useAlias) {
            for (SQLite3Column c: columnsWithoutRowid) {
                c.setUseAlias(true);
                c.setAlias(tableName);
            }
        }
        // concatenate all rows into a single WHERE clause
        List<SQLite3Expression> wherePredicates = new ArrayList<>();
        for (SQLite3RowValue row : rows) {
            SQLite3Expression predicate = generateRectifiedExpression(columnsWithoutRowid, row, false);
            wherePredicates.add(predicate);
        }
        SQLite3Expression whereClause = combinePredicates(wherePredicates);
        selectStatement.setWhereClause(whereClause);
//        List<SQLite3Expression> groupByClause = generateGroupByClause(columnsWithoutRowid, targetRows,
//                allTablesContainOneRow);
//        selectStatement.setGroupByClause(groupByClause);
        return selectStatement;
    }

    private SQLite3Expression combinePredicates(List<SQLite3Expression> predicates) {
        SQLite3Expression combinedPredicate = predicates.get(0);
        for (SQLite3Expression predicate : predicates.subList(1, predicates.size())) {
            combinedPredicate = new Sqlite3BinaryOperation(combinedPredicate, predicate, BinaryOperator.OR);
        }
        return combinedPredicate;
    }


    private Query<SQLConnection> wrapSubQuery(Query<SQLConnection> innerSelectStatement, Query<SQLConnection> outerSelectStatement, String alias) {
        // wrap innerSelectStatement in outerSelectStatement
        String outerSelectStatementString = outerSelectStatement.getQueryString();
        String innerSelectStatementString = innerSelectStatement.getQueryString().substring(0, innerSelectStatement.getQueryString().length() - 1);
        int fromIndex = outerSelectStatementString.toUpperCase().indexOf(" FROM ");
        int whereIndex = outerSelectStatementString.toUpperCase().indexOf(" WHERE ");
        String beforeFrom = outerSelectStatementString.substring(0, fromIndex + " FROM ".length());
        String afterFrom = outerSelectStatementString.substring(whereIndex);
//        String wrappedQuery = beforeFrom + " (" + innerSelectStatementString + afterFrom;
        String wrappedQuery = beforeFrom + " (" + innerSelectStatementString + ") AS " + alias + afterFrom;

        return new SQLQueryAdapter(wrappedQuery, errors);
    }
    private String wrapSubQueryString(String innerSelectStatement, String outerSelectStatement, String alias) {
        // wrap innerSelectStatement in outerSelectStatement
        innerSelectStatement = innerSelectStatement.substring(0, innerSelectStatement.length() - 1);
        int fromIndex = outerSelectStatement.toUpperCase().indexOf(" FROM ");
        int whereIndex = outerSelectStatement.toUpperCase().indexOf(" WHERE ");
        String beforeFrom = outerSelectStatement.substring(0, fromIndex + " FROM ".length());
        String afterFrom = outerSelectStatement.substring(whereIndex);
//        String wrappedQuery = beforeFrom + " (" + innerSelectStatementString + afterFrom;
        String wrappedQuery = beforeFrom + " (" + innerSelectStatement + ") AS " + alias + afterFrom;
        return wrappedQuery;
    }


    private SQLite3Expression generateRectifiedExpression(List<SQLite3Column> columns, SQLite3RowValue pivotRow,
                                                          boolean allowAggregates) {

        SQLite3ExpressionGenerator gen = new SQLite3ExpressionGenerator(state).setRowValue(pivotRow)
                .setColumns(columns);
        if (allowAggregates) {
            gen = gen.allowAggregateFunctions();
        }
        SQLite3Expression expr = gen.generateResultKnownExpression();
        SQLite3Expression rectifiedPredicate;
        if (expr.getExpectedValue().isNull()) {
            // the expr evaluates to NULL => rectify to "expr IS NULL"
            rectifiedPredicate = new SQLite3Expression.SQLite3PostfixUnaryOperation(SQLite3Expression.SQLite3PostfixUnaryOperation.PostfixUnaryOperator.ISNULL, expr);
        } else if (SQLite3Cast.isTrue(expr.getExpectedValue()).get()) {
            // the expr evaluates to TRUE => we can directly return it
            rectifiedPredicate = expr;
        } else {
            // the expr evaluates to FALSE 0> rectify to "NOT expr"
            rectifiedPredicate = new SQLite3UnaryOperation(SQLite3UnaryOperation.UnaryOperator.NOT, expr);
        }
        rectifiedPredicates.add(rectifiedPredicate);
        return rectifiedPredicate;
    }

    private List<SQLite3Expression> getColExpressions(boolean testAggregateFunctions, List<SQLite3Column> columns, boolean useAlias, String tableName) {
        List<SQLite3Expression> colExpressions = new ArrayList<>();

        for (SQLite3Column c : fetchColumns) {
            SQLite3Expression colName = new SQLite3Expression.SQLite3ColumnName(c, targetRow.getValues().get(c));
            if (testAggregateFunctions && Randomly.getBoolean()) {
                SQLite3Aggregate.SQLite3AggregateFunction aggFunc = SQLite3Aggregate.SQLite3AggregateFunction.getRandom(c.getType());
                colName = new SQLite3Aggregate(Arrays.asList(colName), aggFunc);
                if (Randomly.getBoolean()) {
                    colName = generateWindowFunction(columns, colName, true);
                }
                errors.add("second argument to nth_value must be a positive integer");
            }
            // remove the table name from the column name
            if (useAlias) {
                SQLite3ColumnName colNameWithAlias = (SQLite3ColumnName)colName;
                colNameWithAlias.setUseAlias(true);
                colNameWithAlias.setAlias(tableName);
            }
            colExpressions.add(colName);
        }
        if (testAggregateFunctions) {
            SQLite3WindowFunction windowFunction = SQLite3WindowFunction.getRandom(columns, state);
            SQLite3Expression windowExpr = generateWindowFunction(columns, windowFunction, false);
            colExpressions.add(windowExpr);
        }
        for (SQLite3Expression expr : colExpressions) {
            if (expr.getExpectedValue() == null) {
                throw new IgnoreMeException();
            }
        }
        return colExpressions;
    }

    private SQLite3Expression generateWindowFunction(List<SQLite3Column> columns, SQLite3Expression colName,
                                                     boolean allowFilter) {
        StringBuilder sb = new StringBuilder();
        if (Randomly.getBoolean() && allowFilter) {
            appendFilter(columns, sb);
        }
        sb.append(" OVER ");
        sb.append("(");
        if (Randomly.getBoolean()) {
            appendPartitionBy(columns, sb);
        }
        if (Randomly.getBoolean()) {
            sb.append(SQLite3Common.getOrderByAsString(columns, state));
        }
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("RANGE", "ROWS", "GROUPS"));
            sb.append(" ");
            switch (Randomly.fromOptions(FrameSpec.values())) {
                case BETWEEN:
                    sb.append("BETWEEN");
                    sb.append(" UNBOUNDED PRECEDING AND CURRENT ROW");
                    break;
                case UNBOUNDED_PRECEDING:
                    sb.append("UNBOUNDED PRECEDING");
                    break;
                case CURRENT_ROW:
                    sb.append("CURRENT ROW");
                    break;
                default:
                    throw new AssertionError();
            }
            if (Randomly.getBoolean()) {
                sb.append(" EXCLUDE ");
                sb.append(Randomly.fromOptions("NO OTHERS", "TIES"));
            }
        }
        sb.append(")");
        SQLite3Expression.SQLite3PostfixText windowFunction = new SQLite3Expression.SQLite3PostfixText(colName, sb.toString(), colName.getExpectedValue());
        errors.add("misuse of aggregate");
        return windowFunction;
    }

    private void appendFilter(List<SQLite3Column> columns, StringBuilder sb) {
        sb.append(" FILTER (WHERE ");
        sb.append(SQLite3Visitor.asString(generateRectifiedExpression(columns, targetRow, false)));
        sb.append(")");
    }

    private void appendPartitionBy(List<SQLite3Column> columns, StringBuilder sb) {
        sb.append(" PARTITION BY ");
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            String orderingTerm;
            do {
                orderingTerm = SQLite3Common.getOrderingTerm(columns, state);
            } while (orderingTerm.contains("ASC") || orderingTerm.contains("DESC"));
            // TODO investigate
            sb.append(orderingTerm);
        }
    }

    private enum FrameSpec {
        BETWEEN, UNBOUNDED_PRECEDING, CURRENT_ROW
    }

    private List<SQLite3Expression> generateGroupByClause(List<SQLite3Column> columns, SQLite3RowValue rw,
                                                          boolean allTablesContainOneRow) {
        errors.add("GROUP BY term out of range");
        if (allTablesContainOneRow && Randomly.getBoolean()) {
            List<SQLite3Expression> collect = new ArrayList<>();
            for (int i = 0; i < Randomly.smallNumber(); i++) {
                collect.add(new SQLite3ExpressionGenerator(state).setColumns(columns).setRowValue(rw)
                        .generateExpression());
            }
            return collect;
        }
        if (Randomly.getBoolean()) {
            // ensure that we GROUP BY all columns
            List<SQLite3Expression> collect = columns.stream().map(c -> new SQLite3Expression.SQLite3ColumnName(c, rw.getValues().get(c)))
                    .collect(Collectors.toList());
            if (Randomly.getBoolean()) {
                for (int i = 0; i < Randomly.smallNumber(); i++) {
                    collect.add(new SQLite3ExpressionGenerator(state).setColumns(columns).setRowValue(rw)
                            .generateExpression());
                }
            }
            return collect;
        } else {
            return Collections.emptyList();
        }
    }

    private SQLite3Expression generateLimit(long l) {
        if (Randomly.getBoolean()) {
            return SQLite3Constant.createIntConstant(state.getRandomly().getLong(l, Long.MAX_VALUE));
        } else {
            return null;
        }
    }

}
