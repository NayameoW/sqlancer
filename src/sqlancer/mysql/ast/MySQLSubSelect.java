package sqlancer.mysql.ast;

import sqlancer.common.ast.SelectBase;

import java.util.Collections;
import java.util.List;

public class MySQLSubSelect extends SelectBase<MySQLExpression> implements MySQLExpression {
    private SelectType fromOptions = SelectType.ALL;
    private List<String> modifiers = Collections.emptyList();
    private SubqueryType subqueryType;

    public enum SelectType {
        DISTINCT, ALL, DISTINCTROW;
    }

    public enum SubqueryType {
        SCALAR, ROW, TABLE, CORRELATED
    }

    public void setSubqueryType(SubqueryType subqueryType) { this.subqueryType = subqueryType; }

    public SubqueryType getSubqueryType(SubqueryType subqueryType) { return subqueryType; }

    public void setSelectType(SelectType fromOptions) {
        this.setFromOptions(fromOptions);
    }

    public SelectType getFromOptions() {
        return fromOptions;
    }

    public void setFromOptions(SelectType fromOptions) {
        this.fromOptions = fromOptions;
    }

    public void setModifiers(List<String> modifiers) {
        this.modifiers = modifiers;
    }

    public List<String> getModifiers() {
        return modifiers;
    }

    @Override
    public MySQLConstant getExpectedValue() {
        return null;
    }

}
