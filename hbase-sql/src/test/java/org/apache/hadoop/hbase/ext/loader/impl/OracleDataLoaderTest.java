package org.apache.hadoop.hbase.ext.loader.impl;

import org.apache.hadoop.hbase.ext.loader.RelationalDatabaseLoader;
import org.junit.Test;

public class OracleDataLoaderTest {
    RelationalDatabaseLoader loader = new OracleDataLoader();

    @Test
    public void testLoadTable() {
        String tableName = "report1";
        this.loader.loadTable(tableName, new String[] { "TIME_ID", "AREA_ID",
                "SVC_BRND_ID" });
    }
}
