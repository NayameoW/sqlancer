package sqlancer.postgres.oracle;

import sqlancer.Randomly;
import sqlancer.Reproducer;
import sqlancer.SQLConnection;
import sqlancer.common.oracle.SubBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.Query;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.PostgresSchema.PostgresTables;
import sqlancer.postgres.PostgresSchema.PostgresRowValue;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresVisitor;
import sqlancer.postgres.ast.*;
import sqlancer.postgres.ast.PostgresAggregate.PostgresAggregateFunction;
import sqlancer.postgres.ast.PostgresSelect.PostgresSubquery;
import sqlancer.postgres.ast.PostgresSelect.PostgresFromTable;
import sqlancer.postgres.gen.PostgresExpressionGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class PostgresSubOracle extends SubBase<PostgresGlobalState, PostgresRowValue, PostgresExpression, SQLConnection> implements TestOracle<PostgresGlobalState> {

    private final PostgresSchema s;
    private Reproducer<PostgresGlobalState> reproducer;

    PostgresExpressionGenerator gen;

    public PostgresSubOracle(PostgresGlobalState state) {
        super(state);
        this.s = state.getSchema();
    }

    @Override
    public void check() throws Exception {
        PostgresTables randomFromTables = s.getRandomTableNonEmptyTables();
        List<PostgresTable> tables = randomFromTables.getTables();
        List<PostgresColumn> columns = randomFromTables.getColumns();
        gen = new PostgresExpressionGenerator(state).setColumns(columns);
        fetchColExpression = columns.stream().map(PostgresColumnReference::new)
                .collect(Collectors.toList());

        // generate test cases
        PostgresSubquery subquery = PostgresExpressionGenerator.createSubquery(state, "st", randomFromTables);

        List<PostgresExpression> fromTableRefs = getTableRefs(tables);
        fromTableRefs.add(subquery);
        PostgresSelect testQuery = generateScalarSubquery(fromTableRefs);

        if (options.logEachSelect()) {
            logger.writeCurrent(PostgresVisitor.asString(testQuery));
        }

//        PostgresSelect randomSelect = generateRandomSelect(fromTableRefs);

        // execution
//        Query<SQLConnection> queryAdapter = new SQLQueryAdapter(PostgresVisitor.asString(testQuery));
//        try (SQLancerResultSet rs = state.executeStatementAndGet(queryAdapter)) {
//            int count = 0;
//            while (rs.next()) {
//                count ++;
//            }
//            System.out.println(count);
//        } catch (Exception e) {
//            throw new AssertionError(e);
//        }
    }

    private PostgresExpression generateBooleanExpression(int depth) {
        return null;
    }

    /**
     * Generate a complex SELECT query.
     * @param fromList from which the query selects rows
     * @return a randomly generated SELECT query
     */
    private PostgresSelect generateRandomSelect(List<PostgresExpression> fromList) {
        PostgresSelect select = new PostgresSelect();

        select.setFetchColumns(gen.generateExpressions(Randomly.smallNumber() + 2));
        select.setFromList(fromList);

        // add WHERE
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression(PostgresDataType.BOOLEAN));
        }

        // add ORDER BY
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByClauses(gen.generateOrderBy());
        }

        // add LIMIT
        if (Randomly.getBoolean()) {
            select.setLimitClause(PostgresConstant.createIntConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
            if (Randomly.getBoolean()) {
                select.setOffsetClause(PostgresConstant.createIntConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
            }
        }

        logger.writeCurrent(PostgresVisitor.asString(select));
        return select;
    }

    /**
     * Generate a subquery that returns a table with multiple columns.
     * @param fromList from which the query selects rows
     * @return a table subquery
     */
    private PostgresSelect generateTableSubquery(List<PostgresExpression> fromList) {
        PostgresSelect select = generateRandomSelect(fromList);
        while (select.getFetchColumns().size() <= 1) {
            select = generateRandomSelect(fromList);
        }
        return select;
    }

    /**
     * Generate a subquery that returns exactly one row based on a table subquery.
     * @param fromList from which the query selects rows
     * @return a row subquery
     */
    private PostgresSelect generateRowSubquery(List<PostgresExpression> fromList) {
        PostgresSelect select = new PostgresSelect();
        List<PostgresExpression> rowFromList = new ArrayList<>(fromList);
        rowFromList.add(generateTableSubquery(fromList));

        select.setFetchColumns(fetchColExpression);
        select.setFromList(rowFromList);

        return select;
    }

    /**
     * Generate a scalar subquery.
     * @param fromList from which the query selects rows
     * @return a scalar subquery
     */
    private PostgresSelect generateScalarSubquery(List<PostgresExpression> fromList) {
        PostgresSelect scalarSubquery = new PostgresSelect();

        PostgresSelect rowSubquery = generateRowSubquery(fromList);
        List<PostgresExpression> scalarFromList = new ArrayList<>(fromList);
        scalarFromList.add(rowSubquery);
        scalarSubquery.setFromList(scalarFromList);

        List<PostgresExpression> rowFetchList = rowSubquery.getFetchColumns();
        List<PostgresExpression> scalarFetchList = Randomly.nonEmptySubset(rowFetchList, 1);
        scalarSubquery.setFetchColumns(scalarFetchList);

        // add a LIMIT 1
        scalarSubquery.setLimitClause(PostgresConstant.createIntConstant(1));
        if (Randomly.getBoolean()) {
            scalarSubquery.setOffsetClause(
                    PostgresConstant.createIntConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
        }

        return scalarSubquery;
    }

    // Generation of aggregate functions

    private PostgresExpression getAggregate(PostgresDataType dataType) {
        List<PostgresAggregateFunction> aggregates = PostgresAggregateFunction.getAggregates(dataType);
        PostgresAggregateFunction aggregate = Randomly.fromList(aggregates);
        return generateArgsForAggregate(dataType, aggregate);
    }

    private PostgresAggregate generateArgsForAggregate(PostgresDataType dataType, PostgresAggregateFunction agg) {
        List<PostgresDataType> types = agg.getTypes(dataType);
        List<PostgresExpression> args = new ArrayList<>();
        PostgresExpressionGenerator generator = new PostgresExpressionGenerator(state);
        for (PostgresDataType argType : types) {
            args.add(generator.generateExpression(0, argType));
        }
        return new PostgresAggregate(args, agg);
    }

    // generate EXISTS subquery
    private PostgresExists generateExistsSubquery(List<PostgresExpression> fromList) {
        PostgresSelect select = generateRandomSelect(fromList);
        PostgresExists exists = new PostgresExists(select);
        return exists;
    }

    public List<PostgresExpression> getTableRefs(List<PostgresTable> targetTables) {
        return targetTables.stream().map(t -> new PostgresFromTable(t, Randomly.getBoolean()))
                .collect(Collectors.toList());
    }

    @Override
    public Reproducer<PostgresGlobalState> getLastReproducer() {
        return reproducer;
    }

    @Override
    public String getLastQueryString() {
        return "";
    }
}
