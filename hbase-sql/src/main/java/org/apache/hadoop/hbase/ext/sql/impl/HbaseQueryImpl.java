/*
 * Copyright 
 */

package org.apache.hadoop.hbase.ext.sql.impl;


import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLSyntaxErrorException;
import java.util.List;

import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.select.Select;

import org.apache.commons.beanutils.DynaBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ext.sql.HbaseQuery;
import org.apache.hadoop.hbase.ext.sql.QueryUtil;

/**
 * 通过解析SQL语句的方式实现scan hbase的数据
 * 
 * @author Sun
 * @version HbaseQueryImpl.java 2013-1-7 上午10:54:12
 * @see <a href="http://blog.hummingbird-one.com/?p=10196">利用hbase的coprocessor机制来在hbase上增加sql解析引擎–(一)原因&架构</a>
 */
public class HbaseQueryImpl implements HbaseQuery {
    private Configuration conf = HBaseConfiguration.create();

    @Override
    public List<DynaBean> select(String sql) throws SQLSyntaxErrorException, IOException {
        return select(sql, null, null);
    }

    @Override
    public List<DynaBean> select(String sql, String startRow,
            String stopRow) throws IOException, SQLSyntaxErrorException {
        SelectSqlVisitor sqlVisitor = parseSql(sql);

        HTable table = new HTable(this.conf, sqlVisitor.getTableName());
        Scan scan = sqlVisitor.getScan(startRow, stopRow);
        // HBase 0.95-SNAPSHOT API
        // Scan.setMaxResultSize

        ResultScanner resultScanner = table.getScanner(scan);
        List<DynaBean> rows = QueryUtil.getRows(resultScanner,
                sqlVisitor.getOffset(), sqlVisitor.getLimit());
        resultScanner.close();
        return rows;
    }

    private SelectSqlVisitor parseSql(String sql) throws SQLSyntaxErrorException {
        CCJSqlParserManager parserManager = new CCJSqlParserManager();
        SelectSqlVisitor sqlFinder = null;
        try {
            Select select = (Select) parserManager.parse(new StringReader(sql));
            sqlFinder = new SelectSqlVisitor(select);
        } catch (Exception e) {
            throw new SQLSyntaxErrorException(sql, e);
        }
        return sqlFinder;
    }
}
