/*
 * Copyright
 */

package org.apache.hadoop.hbase.ext.loader.impl;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.hadoop.hbase.ext.HbaseUtil;
import org.apache.hadoop.hbase.ext.loader.RelationalDatabaseLoader;

/**
 * 实现从oracle中导入数据到hbase
 * 
 * @author Sun
 * @version OracleDataLoader.java 2013-1-5 下午4:30:43
 * @see <a href="http://blog.csdn.net/kirayuan/article/details/6371635">Hbase几种数据入库（load）方式比较</a>
 */
public class OracleDataLoader implements RelationalDatabaseLoader {
    private ResultSetHandler<List<Map<String, Object>>> resultSetHandler = new ListMapHandler();

    @Override
    public void loadTable(String tableName, String[] pkColumnNames) {
        List<Map<String, Object>> rows = query(tableName);
        HbaseUtil.create(tableName, new String[] { DEFAULT_COLUMN_FAMILY });
        HbaseUtil.put(tableName, DEFAULT_COLUMN_FAMILY, rows, pkColumnNames);
    }

    private List<Map<String, Object>> query(String tableName) {
        QueryRunner run = new QueryRunner();
        Connection connection = getConnection();

        String sql = String.format("select * from %s", tableName);
        List<Map<String, Object>> rows = null;
        try {
            rows = run.query(connection, sql, this.resultSetHandler);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                DbUtils.close(connection);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return rows;
    }

    private Connection getConnection() {
        Connection conn = null;

        String url = "jdbc:oracle:thin:@192.168.141.10:1521:orcl10g";
        String username = "report";
        String password = "123456";
        try {
            Class.forName("oracle.jdbc.OracleDriver");
            conn = DriverManager.getConnection(url, username, password);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }
}
