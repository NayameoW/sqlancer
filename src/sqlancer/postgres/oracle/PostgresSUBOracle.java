package sqlancer.postgres.oracle;

import sqlancer.Randomly;
import sqlancer.Reproducer;
import sqlancer.SQLConnection;
import sqlancer.common.oracle.SubBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.PostgresSchema.PostgresTables;
import sqlancer.postgres.PostgresSchema.PostgresRowValue;
import sqlancer.postgres.PostgresVisitor;
import sqlancer.postgres.ast.PostgresAggregate;
import sqlancer.postgres.ast.PostgresAggregate.PostgresAggregateFunction;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.ast.PostgresSelect;
import sqlancer.postgres.ast.PostgresSelect.PostgresSubquery;
import sqlancer.postgres.gen.PostgresExpressionGenerator;

import java.util.ArrayList;
import java.util.List;

public class PostgresSUBOracle extends SubBase<PostgresGlobalState, PostgresRowValue, PostgresExpression, SQLConnection> implements TestOracle<PostgresGlobalState> {

    private final PostgresSchema s;
    private Reproducer<PostgresGlobalState> reproducer;

    public PostgresSUBOracle(PostgresGlobalState state) {
        super(state);
        this.s = state.getSchema();
    }

    @Override
    public void check() throws Exception {
        PostgresTables randomFromTables = s.getRandomTableNonEmptyTables();
        List<PostgresTable> tables = randomFromTables.getTables();
        PostgresSubquery subquery = PostgresExpressionGenerator.createSubquery(state, "st", randomFromTables);

        if (options.logEachSelect()) {
            logger.writeCurrent(PostgresVisitor.asString(subquery));
        }
    }

    private PostgresExpression generateBooleanExpression(int depth) {

        return null;
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
