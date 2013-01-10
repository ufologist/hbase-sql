/*
 * Copyright 
 */

package org.apache.hadoop.hbase.ext;

import org.apache.hadoop.hbase.ext.loader.impl.OracleDataLoaderTest;
import org.apache.hadoop.hbase.ext.sql.impl.HbaseQueryImplTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * 
 * @author Sun
 * @version HbaseSqlTestSuite.java 2013-1-9 下午4:21:53
 */
@RunWith(Suite.class)
@SuiteClasses({ OracleDataLoaderTest.class, HbaseQueryImplTest.class })
public class HbaseSqlTestSuite {
}
