package sqlancer.mysql.oracle;

import com.google.common.collect.Lists;
import sqlancer.Randomly;
import sqlancer.Reproducer;
import sqlancer.SQLConnection;
import sqlancer.StateToReproduce.OracleRunReproductionState;
import sqlancer.common.oracle.SubBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.mysql.MySQLErrors;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLSchema;
import sqlancer.mysql.MySQLSchema.*;
import sqlancer.mysql.MySQLVisitor;
import sqlancer.mysql.ast.*;
import sqlancer.mysql.gen.MySQLRandomQuerySynthesizer;
import sqlancer.mysql.ast.MySQLOrderByTerm.MySQLOrder;
import sqlancer.mysql.gen.MySQLExpressionGenerator;


import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class MySQLSubOracle extends SubBase<MySQLGlobalState, MySQLRowValue, MySQLExpression, SQLConnection> implements TestOracle<MySQLGlobalState> {

    private final MySQLSchema s;
    private Reproducer<MySQLGlobalState> reproducer;
    private OracleRunReproductionState localState;
    private List<MySQLExpression> fetchColumns;
    private MySQLExpressionGenerator gen;

    public MySQLSubOracle(MySQLGlobalState state) {
        super(state);
        this.s = state.getSchema();
        // todo: init MySQLErrors
    }

    @Override
    public void check() throws Exception {
        MySQLTables randomFromTables = state.getSchema().getRandomTableNonEmptyTables();
        List<MySQLTable> tables = randomFromTables.getTables();
        List<MySQLColumn> columns = randomFromTables.getColumns();

        gen = new MySQLExpressionGenerator(state).setColumns(randomFromTables.getColumns());

        MySQLSelect subquery = new MySQLSelect();
        subquery.setFromList(tables.stream().map(MySQLTableReference::new).collect(Collectors.toList()));
        fetchColumns = columns.stream().map(c -> new MySQLColumnReference(c, null)).collect(Collectors.toList());
        subquery.setFetchColumns(fetchColumns);

//        MySQLSelect subSelect = MySQLRandomQuerySynthesizer.generateFromSubquery(state, 2);
        List<MySQLExpression> fromList =tables.stream().map(MySQLTableReference::new).collect(Collectors.toList());
        MySQLSelect subSelect = generateRandomSelect(tables.stream().map(MySQLTableReference::new).collect(Collectors.toList()), 3);
        subquery.setFromList(Lists.newArrayList(subSelect));

        MySQLSelect selectExist = generateExistQuery(fromList);

        if (state.getOptions().logEachSelect()) {
            logger.writeCurrent(MySQLVisitor.asString(selectExist));
        }

    }

    private MySQLSelect generateRandomSelect(List<MySQLExpression> fromList, int nr) {
        MySQLSelect selectQuery = new MySQLSelect();
        selectQuery.setFetchColumns(fetchColumns);
        selectQuery.setFromList(fromList);
        MySQLOrderByTerm orderByTerm = new MySQLOrderByTerm(null, MySQLOrder.RAND);
        selectQuery.setOrderByClauses(Lists.newArrayList(orderByTerm));
        MySQLLimit limit = new MySQLLimit(nr);
        selectQuery.setLimitClause(limit);
        return selectQuery;
    }

    private MySQLSelect generateExistQuery(List<MySQLExpression> fromList) {
        MySQLSelect selectQuery = new MySQLSelect();
        selectQuery.setFetchColumns(fetchColumns);
        selectQuery.setFromList(fromList);

        MySQLSelect existQuery = new MySQLSelect();
        existQuery.setFetchColumns(fetchColumns);
        existQuery.setFromList(fromList);
        existQuery.setWhereClause(gen.generateExpression());
        if (Randomly.getBoolean()) {
            existQuery.setGroupByExpressions(fetchColumns);
        }

        MySQLExists exists = new MySQLExists(existQuery);
        selectQuery.setWhereClause(exists);
        return selectQuery;
    }


    @Override
    public Reproducer<MySQLGlobalState> getLastReproducer() {
        return reproducer;
    }


}
