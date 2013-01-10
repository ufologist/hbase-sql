package org.apache.hadoop.hbase.ext.sql.impl;

import java.io.IOException;
import java.sql.SQLSyntaxErrorException;
import java.util.List;

import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.beanutils.DynaProperty;
import org.apache.hadoop.hbase.ext.sql.HbaseQuery;
import org.junit.Assert;
import org.junit.Test;

public class HbaseQueryImplTest {
    HbaseQuery hbaseQuery = new HbaseQueryImpl();

    @Test
    public void testSelectAsterisk() throws SQLSyntaxErrorException, IOException {
        String sql = "SELECT * FROM report1";
        HbaseQuery hbaseQuery = new HbaseQueryImpl();
        List<DynaBean> rows = hbaseQuery.select(sql);
        printBean(rows);
        Assert.assertEquals(13, rows.size());
    }

    @Test
    public void testWhere() throws SQLSyntaxErrorException, IOException {
        String sql = "SELECT * FROM report1 WHERE TIME_ID = 201206 and AREA_ID = 730";
        HbaseQuery hbaseQuery = new HbaseQueryImpl();
        List<DynaBean> rows = hbaseQuery.select(sql);
        printBean(rows);
        Assert.assertEquals(1, rows.size());
    }

    @Test
    public void testLimit() throws SQLSyntaxErrorException, IOException {
        String sql = "SELECT TIME_ID, AREA_NAME FROM report1 limit 3 offset 2";
        HbaseQuery hbaseQuery = new HbaseQueryImpl();
        List<DynaBean> rows = hbaseQuery.select(sql);
        printBean(rows);
        Assert.assertEquals(3, rows.size());
    }

    private static void printBean(List<DynaBean> beans) {
        DynaProperty[] properties = beans.get(0).getDynaClass()
                .getDynaProperties();
        StringBuilder str = new StringBuilder();
        for (DynaProperty property : properties) {
            str.append(property.getName()).append("\t");
        }
        str.append("\n----------------------------------\n");

        for (DynaBean bean : beans) {
            for (DynaProperty property : properties) {
                str.append(bean.get(property.getName())).append("\t");
            }
            str.append("\n");
        }
        System.out.print(str);
        System.out.println("----------------------------------");
        System.out.println(beans.size());
    }
}
