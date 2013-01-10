/*
 * Copyright 
 */

package org.apache.hadoop.hbase.ext.sql;

import java.io.IOException;
import java.sql.SQLSyntaxErrorException;
import java.util.List;

import org.apache.commons.beanutils.DynaBean;

/**
 * 通过SQL语句操作hbase中的数据
 * 
 * @author Sun
 * @version HbaseQuery.java 2013-1-7 上午10:50:45
 */
public interface HbaseQuery {
    public List<DynaBean> select(String sql) throws SQLSyntaxErrorException, IOException;
    public List<DynaBean> select(String sql, String startRow, String stopRow) throws SQLSyntaxErrorException, IOException;
}
