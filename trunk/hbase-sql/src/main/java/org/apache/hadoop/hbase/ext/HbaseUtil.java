/*
 * Copyright 
 */

package org.apache.hadoop.hbase.ext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 操作hbase的工具类
 * 
 * @author Sun
 * @version HbaseUtil.java 2013-1-6 上午10:18:48
 * @see <a href="http://www.cnblogs.com/panfeng412/archive/2011/08/14/2137984.html">HBase Java客户端编程</a>
 */
public class HbaseUtil {
    private static Configuration conf = HBaseConfiguration.create();

    public static void create(String tableName, String[] cfs) {
        try {
            HBaseAdmin admin = new HBaseAdmin(conf);
            if (admin.tableExists(tableName)) {
                System.out.println("表已经存在！");
            } else {
                HTableDescriptor tableDesc = new HTableDescriptor(tableName);
                for (int i = 0; i < cfs.length; i++) {
                    tableDesc.addFamily(new HColumnDescriptor(cfs[i]));
                }
                admin.createTable(tableDesc);
                System.out.println("表创建成功！");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void put(String tableName, String rowKey, String cf,
            String key, String value) {
        HTable table = null;
        try {
            table = new HTable(conf, tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(cf), Bytes.toBytes(key), Bytes.toBytes(value));
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            closeTable(table);
        }
    }

    public static void put(String tableName, String cf,
            List<Map<String, Object>> rows, String[] pkColumnNames) {
        HTable table = null;
        try {
            table = new HTable(conf, tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        List<Put> puts = new ArrayList<Put>();
        int commitCount = 10000;

        for (Map<String, Object> row : rows) {
            Iterator<String> columnNames = row.keySet().iterator();

            String rowKey = getRowKey(row, pkColumnNames);
            Put put = new Put(Bytes.toBytes(rowKey));
            while (columnNames.hasNext()) {
                String columnName = columnNames.next();
                String columnValue = String.valueOf(row.get(columnName));
                put.add(Bytes.toBytes(cf), Bytes.toBytes(columnName),
                        Bytes.toBytes(columnValue));
                puts.add(put);
            }

            // 每xx条提交一次
            // 防止批量提交数据时OutOfMemoryError错误
            if (puts.size() > commitCount) {
                putList(table, puts);
                puts.clear();
            }
        }

        putList(table, puts);
        closeTable(table);
    }

    private static void putList(HTable table, List<Put> puts) {
        if (puts.size() > 0) {
            try {
                table.put(puts);
                table.flushCommits();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void closeTable(HTable table) {
        if (table != null) {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static String getRowKey(Map<String, Object> row, String[] pkColumnNames) {
        StringBuilder str = new StringBuilder();
        for (String columnName : pkColumnNames) {
            str.append(row.get(columnName));
        }
        return str.toString();
    }

    public static void get(String tableName, String rowKey) {
        try {
            HTable table = new HTable(conf, tableName);
            Get get = new Get(Bytes.toBytes(rowKey));
            Result rs = table.get(get);
            printResult(rs);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void scan(String tableName) {
        try {
            HTable table =new HTable(conf, tableName);
            Scan s = new Scan();
            SingleColumnValueFilter sf1 = new SingleColumnValueFilter(
                    Bytes.toBytes("cf"), Bytes.toBytes("TIME_ID"), CompareOp.EQUAL,
                    new BinaryComparator(Bytes.toBytes("201206")));
            SingleColumnValueFilter sf2 = new SingleColumnValueFilter(
                    Bytes.toBytes("cf"), Bytes.toBytes("AREA_ID"), CompareOp.EQUAL,
                    new BinaryComparator(Bytes.toBytes("730")));
            SingleColumnValueFilter sf3 = new SingleColumnValueFilter(
                    Bytes.toBytes("cf"), Bytes.toBytes("SVC_BRND_ID"), CompareOp.EQUAL,
                    new BinaryComparator(Bytes.toBytes("1")));
            // TIME_ID = 201206 and AREA_ID = 730 and SVC_BRND_ID = 1
            FilterList filter = new FilterList(Operator.MUST_PASS_ALL, sf1,
                    sf2, sf3);
            // RowFilter filter = new RowFilter(CompareOp.EQUAL,
            // new RegexStringComparator("\\d{9}3"));
            s.setFilter(filter);
            ResultScanner rs = table.getScanner(s);
            for (Result r : rs) {
                printResult(r);
            }
            rs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void testPage(String tableName, int offset, int limit) {
        try {
            HTable table =new HTable(conf, tableName);
            Scan s = new Scan();
            ResultScanner scanner = table.getScanner(s);
            // row key有多少, result就有多少
            // 一个唯一的row key表示一条记录(一个Result), 包含多个列
            Result result = null;
            for (int i = 0; (result = scanner.next()) != null; i++) {
                if (i < offset) {
                    continue;
                } else if (i == offset + limit) {
                    break;
                } else {
                    printResult(result);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void printResult(Result result) {
        for (KeyValue kv : result.raw()) {
            System.out.print(new String(kv.getRow()) + "\t");
            System.out.print(new String(kv.getFamily()) + ":");
            System.out.print(new String(kv.getQualifier()) + "\t");
            System.out.print(kv.getTimestamp() + "\t");
            System.out.println(new String(kv.getValue()));
        }
    }
}
