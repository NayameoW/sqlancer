package sqlancer.mysql;

import sqlancer.SQLConnection;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.mysql.ast.MySQLColumnReference;
import sqlancer.mysql.ast.MySQLConstant;
import sqlancer.mysql.ast.MySQLExpression;
import sqlancer.mysql.ast.MySQLSelect;
import sqlancer.common.query.Query;
import sqlancer.mysql.MySQLSchema.MySQLColumn;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class MySQLSubquerySupervisor {

    private final MySQLSelect selectQuery;
    private final MySQLGlobalState state;
    private List<MySQLExpression> columns;

    public MySQLSubquerySupervisor(MySQLSelect selectQuery, MySQLGlobalState state) {
        this.selectQuery = selectQuery;
        this.state = state;
        this.columns = selectQuery.getFetchColumns();
    }

    public Map<Integer, Map<MySQLColumn, MySQLConstant>> getResult() {
        String queryString = MySQLVisitor.asString(selectQuery);
        Query<SQLConnection> queryAdapter = new SQLQueryAdapter(queryString);
        Map<Integer, Map<MySQLColumn, MySQLConstant>> queryResults = new HashMap<>();
        int rowIdx = 0;
        try (SQLancerResultSet resultSet = queryAdapter.executeAndGet(state)) {
            while (resultSet.next()) {
                Map<MySQLColumn, MySQLConstant> rowValues = new HashMap<>();
                for (int i = 0; i < columns.size(); i++) {
                    MySQLColumn column = getColumns().get(i);
                    Object value;
                    int columnIndex = i + 1;
                    MySQLConstant constant;
                    if (resultSet.getString(columnIndex) == null) {
                        constant = MySQLConstant.createNullConstant();
                    } else {
                        switch (column.getType()) {
                            case INT:
                                value = resultSet.getLong(columnIndex);
                                constant = MySQLConstant.createIntConstant((long) value);
                                break;
                            case VARCHAR:
                                value = resultSet.getString(columnIndex);
                                constant = MySQLConstant.createStringConstant((String) value);
                                break;
                            case FLOAT:
                                value = resultSet.getFloat(columnIndex);
                                constant = MySQLConstant.createFloatConstant((float) value);
                                break;
                            case DOUBLE:
                                value = resultSet.getDouble(columnIndex);
                                constant = MySQLConstant.createDoubleConstant((double) value);
                                break;
                            case DECIMAL:
                                value = resultSet.getBigDecimal(columnIndex);
                                constant = MySQLConstant.createDecimalConstant((BigDecimal) value);
                                break;
                            default:
                                throw new AssertionError();
                        }
                    }
                    rowValues.put(column, constant);
                }
                queryResults.put(rowIdx, rowValues);
                rowIdx ++;
            }
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        return queryResults;
    }

    /**
     * Convert data type MySQLExpression to MySQLColumn
     * @return columns
     */
    private List<MySQLColumn> getColumns() {
        List<MySQLColumn> columnList = new ArrayList<>();
        for (MySQLExpression colum: columns){
            assert colum instanceof MySQLColumnReference : colum;
            MySQLColumnReference columnReference = (MySQLColumnReference) colum;
            columnList.add(columnReference.getColumn());
        }
        return columnList;
    }

//    private MySQLTable createTemporaryTable() {
//
//        return new MySQLTable();
//    }


}