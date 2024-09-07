package sqlancer.omnigen;

import sqlancer.SQLGlobalState;

public class DBGlobalState extends SQLGlobalState<DBOptions, DBSchema> {

    @Override
    protected DBSchema readSchema() throws Exception {
        return null;
    }
}
