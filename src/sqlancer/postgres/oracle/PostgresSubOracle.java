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
import sqlancer.postgres.ast.PostgresAggregate;
import sqlancer.postgres.ast.PostgresAggregate.PostgresAggregateFunction;
import sqlancer.postgres.ast.PostgresColumnReference;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.ast.PostgresSelect;
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

        PostgresSelect testQuery = new PostgresSelect();
        testQuery.setFetchColumns(fetchColExpression);
        List<PostgresExpression> fromTableExpressions = tables.stream().map(t -> new PostgresFromTable(t, Randomly.getBoolean()))
                        .collect(Collectors.toList());
        fromTableExpressions.add(subquery);
        testQuery.setFromList(fromTableExpressions);

//        if (options.logEachSelect()) {
//            logger.writeCurrent(PostgresVisitor.asString(testQuery));
//        }

        generateRandomSelect();

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

    private PostgresSelect generateRandomSelect(List<PostgresExpression> fromList) {
        PostgresSelect select = new PostgresSelect();
        List<PostgresExpression> fetchColumns = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber() + 2; i++) {
            if (Randomly.getBoolean()) {
                fetchColumns.add(generateBooleanExpression(i));
            } else {
                fetchColumns.add(Randomly.fromList(fetchColumns));
            }
        }

        select.setFetchColumns(fetchColumns);
        select.setFromList(fromList);

        logger.writeCurrent(PostgresVisitor.asString(select));
        return select;
    }

    private PostgresSelect generateTableSubquery(List<PostgresExpression> fromList) {
        PostgresSelect select = generateRandomSelect(fromList);

        return select;
    }

    private PostgresSelect generateRowSubquery(List<PostgresExpression> fromList) {
        PostgresSelect select = new PostgresSelect();
        List<PostgresExpression> rowFromList = new ArrayList<>(fromList);
        rowFromList.add(generateTableSubquery(fromList));

        select.setFetchColumns(fetchColExpression);
        select.setFromList(rowFromList);

        return select;
    }

    private PostgresSelect generateScalarSubquery(List<PostgresExpression> fromList) {

    }

    // Generation of aggregate functions

    private PostgresExpression getAggregate(PostgresDataType dataType) {
        List<PostgresAggregateFunction> aggregates = PostgresAggregateFunction.getAggregates(dataType);
        PostgresAggregateFunction aggregate = Randomly.fromList(aggregates);
        return generateArgsForAggregate(dataType, aggregate);
    }

    public PostgresAggregate generateArgsForAggregate(PostgresDataType dataType, PostgresAggregateFunction agg) {
        List<PostgresDataType> types = agg.getTypes(dataType);
        List<PostgresExpression> args = new ArrayList<>();
        PostgresExpressionGenerator generator = new PostgresExpressionGenerator(state);
        for (PostgresDataType argType : types) {
            args.add(generator.generateExpression(0, argType));
        }
        return new PostgresAggregate(args, agg);
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
