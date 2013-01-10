/*
 * Copyright 
 */

package org.apache.hadoop.hbase.ext.sql.impl;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 获取SQL select语句的各个部分
 * 
 * @author Sun
 * @version SelectSqlVisitor.java 2013-1-8 上午9:19:50
 */
public class SelectSqlVisitor implements SelectVisitor, FromItemVisitor,
        ExpressionVisitor {
    private List<String> tables = new ArrayList<String>();
    private long limit = 0L;
    private long offset = 0L;

    private Scan scan = new Scan();
    // TODO 目前将全部的where条件都用作and逻辑
    private FilterList filters = new FilterList();

    public SelectSqlVisitor(Select select) {
        // 访问select语句
        select.getSelectBody().accept(this);
    }

    public String getTableName() {
        // XXX 由于针对hbase都是简单查询, 只会有一张表
        return this.tables.get(0);
    }

    public Scan getScan(String startRow, String stopRow) {
        setScanRange(startRow, stopRow);
        this.scan.setFilter(this.filters);
        return this.scan;
    }

    private void setScanRange(String startRow, String stopRow) {
        if (StringUtils.isNotBlank(startRow)) {
            this.scan.setStartRow(Bytes.toBytes(startRow));
        }
        if (StringUtils.isNotBlank(stopRow)) {
            this.scan.setStopRow(Bytes.toBytes(stopRow));
        }
    }
    
    public long getLimit() {
        return this.limit;
    }

    public long getOffset() {
        return this.offset;
    }

    private void setColumn(List selectItems) {
        for (Object item : selectItems) {
            if (item.toString().equals("*")) {
                break;
            }
            // TODO 写死的cf, 没有处理字段别名等等情况
            this.scan.addColumn(Bytes.toBytes("cf"),
                    Bytes.toBytes(item.toString()));
        }
    }

    private void initLimitOffset(PlainSelect plainSelect) {
        Limit limit = plainSelect.getLimit();
        if (limit == null) {
            return;
        }

        this.limit = limit.getRowCount();
        this.offset = limit.getOffset();
    }

    @Override
    public void visit(PlainSelect plainSelect) {
        List selectItems = plainSelect.getSelectItems();
        setColumn(selectItems);
        initLimitOffset(plainSelect);

        plainSelect.getFromItem().accept(this);
        if (plainSelect.getWhere() != null) {
            plainSelect.getWhere().accept(this);
        }
    }

    @Override
    public void visit(Table table) {
        String tableWholeName = table.getWholeTableName();
        this.tables.add(tableWholeName);
    }

    @Override
    public void visit(AndExpression and) {
        and.getLeftExpression().accept(this);
        and.getRightExpression().accept(this);
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        // XXX 目前假设到EqualsTo这层已经是a = 2这样的表达式了
        String column = equalsTo.getLeftExpression().toString();
        String value = equalsTo.getRightExpression().toString();

        // TODO 写死的cf
        SingleColumnValueFilter filter = new SingleColumnValueFilter(
                Bytes.toBytes("cf"), Bytes.toBytes(column),
                CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(value)));
        this.filters.addFilter(filter);
    }

    @Override
    public void visit(Parenthesis arg0) {
        // TODO 未实现
    }

    @Override
    public void visit(OrExpression arg0) {
        // TODO 未实现
    }

    @Override
    public void visit(NotEqualsTo arg0) {
        // TODO 未实现
    }

    @Override
    public void visit(Column column) {
        // TODO 未实现
    }

    public void visit(GreaterThan arg0) {}
    public void visit(GreaterThanEquals arg0) {}
    public void visit(MinorThan arg0) {}
    public void visit(MinorThanEquals arg0) {}
    public void visit(InExpression arg0) {}
    public void visit(LikeExpression arg0) {}
    public void visit(DoubleValue arg0) {}
    public void visit(LongValue arg0) {}
    public void visit(DateValue arg0) {}
    public void visit(TimeValue arg0) {}
    public void visit(TimestampValue arg0) {}
    public void visit(StringValue arg0) {}
    public void visit(Addition arg0) {}
    public void visit(Division arg0) {}
    public void visit(Multiplication arg0) {}
    public void visit(Subtraction arg0) {}
    public void visit(Between arg0) {}
    public void visit(WhenClause whenClause) {}
    public void visit(NullValue arg0) {}
    public void visit(Function arg0) {}
    public void visit(InverseExpression arg0) {}
    public void visit(JdbcParameter arg0) {}
    public void visit(IsNullExpression arg0) {}
    public void visit(CaseExpression arg0) {}
    public void visit(ExistsExpression arg0) {}
    public void visit(AllComparisonExpression arg0) {}
    public void visit(AnyComparisonExpression arg0) {}
    public void visit(Union arg0) {}
    public void visit(SubJoin arg0) {}
    public void visit(SubSelect arg0) {}
    public void visit(BitwiseXor arg0) {}
    public void visit(BitwiseOr arg0) {}
    public void visit(BitwiseAnd arg0) {}
    public void visit(Matches arg0) {}
    public void visit(Concat arg0) {}
}
