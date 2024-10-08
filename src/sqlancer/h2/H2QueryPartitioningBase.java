package sqlancer.h2;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.h2.H2Provider.H2GlobalState;
import sqlancer.h2.H2Schema.H2Column;
import sqlancer.h2.H2Schema.H2Table;
import sqlancer.h2.H2Schema.H2Tables;
import sqlancer.h2.ast.H2ColumnReference;
import sqlancer.h2.ast.H2Expression;
import sqlancer.h2.ast.H2Join;
import sqlancer.h2.ast.H2Select;
import sqlancer.h2.ast.H2TableReference;

public class H2QueryPartitioningBase extends TernaryLogicPartitioningOracleBase<H2Expression, H2GlobalState>
        implements TestOracle<H2GlobalState> {

    H2Schema s;
    H2Tables targetTables;
    H2ExpressionGenerator gen;
    H2Select select;

    public H2QueryPartitioningBase(H2GlobalState state) {
        super(state);
        H2Errors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        gen = new H2ExpressionGenerator(state).setColumns(targetTables.getColumns());
        initializeTernaryPredicateVariants();
        select = new H2Select();
        select.setFetchColumns(generateFetchColumns());
        List<H2Table> tables = targetTables.getTables();
        List<H2TableReference> tableList = tables.stream().map(t -> new H2TableReference(t))
                .collect(Collectors.toList());
        List<H2Expression> joins = H2Join.getJoins(tableList, state);
        select.setJoinList(joins.stream().collect(Collectors.toList()));
        select.setFromList(tableList.stream().collect(Collectors.toList()));
        select.setWhereClause(null);
    }

    List<H2Expression> generateFetchColumns() {
        List<H2Expression> columns = new ArrayList<>();
        columns.add(new H2ColumnReference(new H2Column("*", null)));
        return columns;
    }

    @Override
    protected ExpressionGenerator<H2Expression> getGen() {
        return gen;
    }

}
