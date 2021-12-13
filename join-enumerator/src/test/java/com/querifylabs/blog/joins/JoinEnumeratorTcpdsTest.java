package com.querifylabs.blog.joins;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Estimate the number of joins for the TPC-DS queries.
 */
public class JoinEnumeratorTcpdsTest {
    /** The TPC-DS query 17 contains 211200 valid cross-product free join orders. */
    private static final long TPCDS_17 = 211200;

    @Test
    public void testTpcdsQ17() {
        JoinEnumerator topology = new JoinEnumerator();

        // d1.d_date_sk = ss_sold_date_sk
        topology.addJoinCondition("date_dim d1", "store_sales");

        // i_item_sk = ss_item_sk
        topology.addJoinCondition("item", "store_sales");

        // s_store_sk = ss_store_sk
        topology.addJoinCondition("store", "store_sales");

        // ss_customer_sk = sr_customer_sk
        // ss_item_sk = sr_item_sk
        // ss_ticket_number = sr_ticket_number
        topology.addJoinCondition("store_sales", "store_returns");

        // sr_returned_date_sk = d2.d_date_sk
        topology.addJoinCondition("store_returns", "date_dim d2");

        // sr_customer_sk = cs_bill_customer_sk
        // sr_item_sk = cs_item_sk
        topology.addJoinCondition("store_returns", "catalog_sales");

        // cs_sold_date_sk = d3.d_date_sk
        topology.addJoinCondition("catalog_sales", "date_dim d3");

        assertEquals(TPCDS_17, topology.count());
    }
}
