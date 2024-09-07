package sqlancer.omnigen;

import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.AbstractSchema;

import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.TableIndex;
import sqlancer.omnigen.DBSchema.DBTable;

import java.util.List;

public class DBSchema extends AbstractSchema<DBGlobalState, DBTable> {
    public DBSchema(List<DBTable> databaseTables) {
        super(databaseTables);
    }

    public static class DBTable extends AbstractRelationalTable<DBColumn, DBIndex, DBGlobalState> {

        public DBTable(String name, List<DBColumn> columns, List<DBIndex> indexes, boolean isView) {
            super(name, columns, indexes, isView);
        }
    }

    public static class DBColumn extends AbstractTableColumn<DBTable, DBDataType> {

        public DBColumn(String name, DBTable table, DBDataType type) {
            super(name, table, type);
        }
    }

    public enum DBDataType{
        INT;
    }

    public static final class DBIndex extends TableIndex{

        private DBIndex(String indexName) {
            super(indexName);
        }
    }

}
