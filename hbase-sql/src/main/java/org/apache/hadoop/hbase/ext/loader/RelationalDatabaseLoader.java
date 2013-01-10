/*
 * Copyright 
 */

package org.apache.hadoop.hbase.ext.loader;

/**
 * 将关系型数据库中的数据加载到hbase
 * 
 * @author Sun
 * @version RelationalDatabaseLoader.java 2013-1-5 下午4:27:13
 */
public interface RelationalDatabaseLoader {
    public final String DEFAULT_COLUMN_FAMILY = "cf";

    /**
     * 将数据库表中的所有数据导入hbase
     * 
     * @param tableName
     * @param pkColumnNames 一个或多个PK字段名, 将PK字段的值组合成row key来存储
     */
    public void loadTable(String tableName, String[] pkColumnNames);
}
