package sqlancer.omnigen;

import sqlancer.common.schema.AbstractSchema;

import java.util.List;

public class DBSchema extends AbstractSchema<DBGlobalState, DBTable> {
    public DBSchema(List<DBTable> databaseTables) {
        super(databaseTables);
    }
}
