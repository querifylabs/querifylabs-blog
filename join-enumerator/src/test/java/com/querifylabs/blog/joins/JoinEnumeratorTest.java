package com.querifylabs.blog.joins;

import org.junit.Test;

import static com.querifylabs.blog.joins.JoinEnumeratorUtils.catalan;
import static com.querifylabs.blog.joins.JoinEnumeratorUtils.fact;
import static org.junit.Assert.assertEquals;

/**
 * Test the estimated number of joins for common join topologies: chain, star, clique.
 */
public class JoinEnumeratorTest {

    private static final int MIN_INPUTS = 2;
    private static final int MAX_INPUTS = 8;

    @Test
    public void testChain() {
        for (int n = MIN_INPUTS; n <= MAX_INPUTS; n++) {
            var topology = new JoinEnumerator();
            for (int i = 1; i < n; i++) {
                topology.addJoinCondition(table(i-1), table(i));
            }
            long expected = (long)Math.pow(2, n-1) * catalan(n - 1);
            assertEquals(expected, topology.count());
        }
    }

    @Test
    public void testStar() {
        for (int n = MIN_INPUTS; n <= MAX_INPUTS; n++) {
            var topology = new JoinEnumerator();
            for (int i = 1; i < n; i++) {
                topology.addJoinCondition(table(0), table(i));
            }
            long expected = (long)Math.pow(2, n-1) * fact(n - 1);
            assertEquals(expected, topology.count());
        }
    }

    @Test
    public void testClique() {
        for (int n = MIN_INPUTS; n <= MAX_INPUTS; n++) {
            var topology = new JoinEnumerator();
            for (int i = 0; i < n; i++) {
                for (int j = 0; j < n; j++) {
                    if (i == j) {
                        continue;
                    }
                    topology.addJoinCondition(table(i), table(j));
                }
            }
            long expected = fact(n) * catalan(n - 1);
            assertEquals(expected, topology.count());
        }
    }

    private static String table(int index) {
        return "t" + index;
    }
}
