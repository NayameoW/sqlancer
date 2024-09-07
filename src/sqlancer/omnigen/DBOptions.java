package sqlancer.omnigen;

import sqlancer.DBMSSpecificOptions;
import sqlancer.OracleFactory;
import sqlancer.common.oracle.TestOracle;
import sqlancer.omnigen.DBOptions.DBOracleFactory;

import java.util.List;

public class DBOptions implements DBMSSpecificOptions<DBOracleFactory> {

    @Override
    public List getTestOracleFactory() {
        return List.of();
    }

    public enum DBOracleFactory implements OracleFactory<DBGlobalState> {
        ;


        @Override
        public TestOracle<DBGlobalState> create(DBGlobalState globalState) throws Exception {
            return null;
        }

        @Override
        public boolean requiresAllTablesToContainRows() {
            return OracleFactory.super.requiresAllTablesToContainRows();
        }
    }
}
