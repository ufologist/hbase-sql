/*
 * Copyright 
 */

package org.apache.hadoop.hbase.ext.loader.impl;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.dbutils.ResultSetHandler;

/**
 * 将ResultSet封装成ListMap结构的数据
 * 一个list表示一行数据, map中存放该行各个列的值
 * 
 * @author Sun
 * @version ListMapHandler.java 2013-1-6 上午10:06:57
 */
public class ListMapHandler implements ResultSetHandler<List<Map<String, Object>>> {
    public List<Map<String, Object>> handle(ResultSet rs) throws SQLException {
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        ResultSetMetaData meta = rs.getMetaData();

        int colCount = meta.getColumnCount();
        while (rs.next()) {
            Map<String, Object> row = new HashMap<String, Object>();
            for (int i = 0; i < colCount; i++) {
                String columnName = meta.getColumnName(i + 1);
                Object columnValue = rs.getObject(columnName);
                row.put(columnName, columnValue);
            }
            rows.add(row);
        }

        return rows;
    }
}
