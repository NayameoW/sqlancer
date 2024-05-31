package sqlancer.mysql.oracle;

import com.google.common.collect.Lists;
import io.questdb.cairo.pool.ReaderPool;
import sqlancer.*;
import sqlancer.common.oracle.SubBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.Query;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.mysql.*;
import sqlancer.mysql.MySQLSchema.*;
import sqlancer.mysql.ast.*;
import sqlancer.mysql.ast.MySQLOrderByTerm.MySQLOrder;
import sqlancer.mysql.gen.MySQLExpressionGenerator;
import sqlancer.mysql.ast.MySQLBinaryComparisonOperation.BinaryComparisonOperator;
import sqlancer.mysql.gen.MySQLRandomQuerySynthesizer;


import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class MySQLSubOracle extends SubBase<MySQLGlobalState, MySQLRowValue, MySQLExpression, SQLConnection> implements TestOracle<MySQLGlobalState> {

    private final MySQLSchema s;
    private Reproducer<MySQLGlobalState> reproducer;
    private List<MySQLExpression> fetchColumns;
    private MySQLExpressionGenerator gen;

    private enum SubqueryType {
        SCALAR, EXISTS, IN, FROM
    }

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

        List<MySQLExpression> fromList =tables.stream().map(MySQLTableReference::new).collect(Collectors.toList());
        MySQLSelect subSelect = generateRandomSelect(tables.stream().map(MySQLTableReference::new).collect(Collectors.toList()), 3);
        subquery.setFromList(Lists.newArrayList(subSelect));

        MySQLSelect testSubquery;

//        switch (Randomly.fromOptions(SubqueryType.SCALAR, SubqueryType.EXISTS, SubqueryType.IN)) {
//            case SCALAR:
//                testSubquery = generateWhereSubquery(fromList, columns);
//                break;
//            case EXISTS:
//                testSubquery = generateExistQuery(fromList);
//                break;
//            case IN:
//                testSubquery = generateInQuery(fromList);
//                break;
//            default:
//                throw new AssertionError();
//        }

//        testSubquery = generateWhereSubquery(fromList, columns);
        testSubquery = generateScalarSubquery(fromList, columns);
//        testSubquery = generateExistQuery(fromList, 3);
//        testSubquery = generateOnlyWhereSubquery();

        if (state.getOptions().logEachSelect()) {
            logger.writeCurrent(MySQLVisitor.asString(testSubquery));
//            logger.writeCurrent(testString);
//            logger.writeCurrent(testString2);
//            logger.writeCurrent(String.valueOf(rootNode.getNodeNum()));
//            if (rootNode.getCreateTableSQL() != null) {
//                logger.writeCurrent(rootNode.getCreateTableSQL());
//            }
//            if (rootNode.getInsertValuesSQL() != null) {
//                logger.writeCurrent(rootNode.getInsertValuesSQL());
//            }
//            logger.writeCurrent(visitor.getTableString());
        }

        MySQLSubqueryTreeNode rootNode = generateSubqueryTree(testSubquery);
        MySQLTemporaryTableManager manager = new MySQLTemporaryTableManager();
        MySQLSubqueryTreeNodeVisitor visitor = new MySQLSubqueryTreeNodeVisitor();
        visitor.visit(rootNode);

        // testing oracle
        int subqueryCount = 0;
        Query<SQLConnection> subqueryAdapter = new SQLQueryAdapter(MySQLVisitor.asString(testSubquery));
        try (SQLancerResultSet rs = state.executeStatementAndGet(subqueryAdapter)) {
            if (rs == null) {
                System.out.println("no results");
                throw new IgnoreMeException();
            } else {
                while (rs.next()) {
                    subqueryCount++;
                }
            }
//            System.out.println("SELECT " + subqueryCount + " results");
        } catch (Exception e) {
            throw new AssertionError(e);
        }

        // execute flattened queries
        int flattenedCount = 0;
        String[] statements = visitor.getTableString().split(";");
        for (int i = 0; i < statements.length; i++) {
            String statement = statements[i];
            Query<SQLConnection> tableGenerator = new SQLQueryAdapter(statement.trim());
            if (statement.startsWith("SELECT") && i == statements.length - 1) {
                try (SQLancerResultSet rs = state.executeStatementAndGet(tableGenerator)) {
                    if (rs == null) {
                        System.out.println("no results");
                        throw new IgnoreMeException();
                    } else {
                        while (rs.next()) {
                            flattenedCount ++;
                        }
                    }
//                    System.out.println("SELECT " + flattenedCount + " results");
//                    System.out.println(statement + " executed successfully");
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            } else {
                if(state.executeStatement(tableGenerator)) {
//                    System.out.println(statement + " executed successfully");
                } else {
//                    System.out.println(statement + " failed");
                }
            }

        }

        // test
//        if (subqueryCount != flattenedCount) {
//            System.out.println(MySQLVisitor.asString(testSubquery));
//            System.out.println("BUG: " + subqueryCount + " != " + flattenedCount);
//        } else {
//            System.out.println(MySQLVisitor.asString(testSubquery));
//            System.out.println(subqueryCount + " == " + flattenedCount);
//        }

        dropAllTempTables(visitor.getTableNames());
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

    private MySQLSelect generateRandomSelectUsingTableName(String tableName) {
        MySQLSelect randomSelect = MySQLRandomQuerySynthesizer.generate(state, 1);

        return randomSelect;
    }

    private MySQLSelect generateExistQuery(List<MySQLExpression> fromList, int depth) {
        MySQLSelect innerQuery = MySQLRandomQuerySynthesizer.generate(state, 2);
        MySQLSelect outerQuery = null;

        for (int i = 0; i < depth; i++) {
            outerQuery = MySQLRandomQuerySynthesizer.generate(state, 2);
            MySQLExists exists = new MySQLExists(innerQuery);
            outerQuery.setWhereClause(exists);
            innerQuery = outerQuery;
        }

        return outerQuery;
    }

    private MySQLSelect generateTableSubquery(List<MySQLExpression> fromList) {
        MySQLSelect tableSubquery = generateRandomSelect(fromList, (int) Randomly.getNotCachedInteger(2, 10));
        return tableSubquery;
    }

    private MySQLSelect generateRowSubquery(MySQLSelect tableSubquery, List<MySQLColumn> columns) {
        MySQLSelect rowSubquery = new MySQLSelect();
        MySQLTable vTable1 = new MySQLTable("st0", columns, null, null);
        List<MySQLExpression> columnExpression = columns.stream().map(c -> MySQLColumnReference.createAlias(c, vTable1)).collect(Collectors.toList());
        rowSubquery.setFetchColumns(columnExpression);
        rowSubquery.setFromList(Lists.newArrayList(tableSubquery));
        MySQLLimit limit = new MySQLLimit(1);
        rowSubquery.setLimitClause(limit);
        rowSubquery.setTableAlias(new MySQLTableAlias(vTable1));

//        if (Randomly.getBoolean()) {
//            MySQLTable vTable2 = new MySQLTable("st0", columns, null, null);
//            MySQLExpressionGenerator generator = new MySQLExpressionGenerator(state).setColumns(columns);
//            generator.setAliasTable(vTable2);
//            MySQLExpression whereClause = generator.generateExpression();
//            rowSubquery.setWhereClause(whereClause);
//        }

        return rowSubquery;
    }

    /**
     * This method generates a complex subquery which finally returns a scalar
     * @return MySQLSelect subquery
     */
    private MySQLSelect generateScalarSubquery(List<MySQLExpression> fromList, List<MySQLColumn> columns) {
        MySQLSelect tableSubquery = generateTableSubquery(fromList);
//        MySQLSelect tableSubquery = MySQLRandomQuerySynthesizer.generate(state, 2);
        MySQLSelect rowSubquery = generateRowSubquery(tableSubquery, columns);

        List<MySQLColumn> singleColumn = Randomly.nonEmptySubset(columns, 1);
        MySQLTable vTable2 = new MySQLTable("st1", columns, null, null);
        List<MySQLExpression> singleColumnExpression = singleColumn.stream().map(c ->  MySQLColumnReference.createAlias(c, vTable2)).collect(Collectors.toList());

        MySQLSelect selectQuery = new MySQLSelect();
        selectQuery.setFetchColumns(singleColumnExpression);
        selectQuery.setFromList(Lists.newArrayList(rowSubquery));
        selectQuery.setTableAlias(new MySQLTableAlias(vTable2));

        return selectQuery;
    }

    private MySQLSelect generateWhereSubquery(List<MySQLExpression> fromList, List<MySQLColumn> columns) {
        MySQLSelect selectQuery = generateScalarSubquery(fromList, columns);
        MySQLSelect scalarSubquery1 = generateScalarSubquery(fromList, columns);
        MySQLSelect scalarSubquery2 = generateScalarSubquery(fromList, columns);
        MySQLExpression whereClause = new MySQLBinaryComparisonOperation(scalarSubquery1, scalarSubquery2, BinaryComparisonOperator.GREATER_EQUALS);

        selectQuery.setWhereClause(whereClause);

        return selectQuery;
    }

    private MySQLSelect generateOnlyWhereSubquery() {
        MySQLSelect selectQuery = MySQLRandomQuerySynthesizer.generate(state, 3);
        MySQLSelect leftSubquery = MySQLRandomQuerySynthesizer.generate(state, 2);
        MySQLSelect rightSubquery = MySQLRandomQuerySynthesizer.generate(state, 2);
        MySQLExpression whereClause = new MySQLBinaryComparisonOperation(leftSubquery, rightSubquery, BinaryComparisonOperator.GREATER_EQUALS);
        selectQuery.setWhereClause(whereClause);

        return selectQuery;
    }

    private MySQLSelect generateInQuery(List<MySQLExpression> fromList) {
        MySQLSelect selectQuery = new MySQLSelect();
        selectQuery.setFetchColumns(fetchColumns);
        selectQuery.setFromList(fromList);
        List<MySQLExpression> elementList = new ArrayList<>();
        MySQLInOperation inOperation = new MySQLInOperation(selectQuery, elementList, true);

        return selectQuery;
    }

    private MySQLSelect generateJoinSubquery(List<MySQLExpression> fromList, List<MySQLExpression> joinList) {

        return new MySQLSelect();
    }

    private MySQLSubqueryTreeNode generateSubqueryTree(MySQLSelect subquery) {
        MySQLSubqueryTreeNode rootNode = new MySQLSubqueryTreeNode(subquery, state);

        for (MySQLExpression fromClause : subquery.getFromList()) {
            if (fromClause instanceof MySQLSelect) {
                rootNode.setFromNode(generateSubqueryTree((MySQLSelect) fromClause));
            }
        }

        if (subquery.getWhereClause() instanceof MySQLBinaryComparisonOperation) {
            MySQLBinaryComparisonOperation whereClause = (MySQLBinaryComparisonOperation) subquery.getWhereClause();
            if (whereClause.getLeft() instanceof MySQLSelect) {
                rootNode.addWhereSubquery(generateSubqueryTree((MySQLSelect) whereClause.getLeft()));
            }
            if (whereClause.getRight() instanceof MySQLSelect) {
                rootNode.addWhereSubquery(generateSubqueryTree((MySQLSelect) whereClause.getRight()));
            }
        }

        return rootNode;
    }

    private void dropAllTempTables(List<String> tableNames) throws Exception {
        for (String tableName : tableNames) {
            StringBuilder sb = new StringBuilder();
            sb.append("DROP TABLE IF EXISTS ").append(tableName);
            sb.append(";");
            Query<SQLConnection> delete = new SQLQueryAdapter(sb.toString());
//            if (state.executeStatement(delete)) {
//                System.out.println(sb.toString());
//            }
        }
    }

    @Override
    public Reproducer<MySQLGlobalState> getLastReproducer() {
        return reproducer;
    }


}