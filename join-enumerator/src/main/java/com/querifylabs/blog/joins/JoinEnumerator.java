package com.querifylabs.blog.joins;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A naive implementation of a join enumerator with cross-product suppression.
 * <p>
 * Constructs the possible bushy parenthesizations for the N inputs. Then,
 * creates possible orders of leaves. Each parenthesizations is combined
 * with each order of leaves, and then checked for the presence of cross-products.
 * <p>
 * The algorithm assumes that every join is an inner-join.
 * <p>
 * The algorithm is very simple and convenient for the educational purposes. However,
 * it is very inefficient and cannot be used to plan join graphs with more than eight
 * tables.
 * <p>
 * Consider the join graph A-B-C. There valid parenthesizations are ((T1xT2)xT3)
 * (T1x(T2xT3)). The valid lead orders are ABC, ACB, BAC, BCA, CAB, CBA. Combining
 * these two we got 12 bushy join orders. Cross-product are not present in the
 * following join orders: (AB)C, A(BC), A(CB), (BA)C, (BC)A, C(AB), (CB)A, C(BA),
 * giving us 8 cross-product free join orders.
 */
public class JoinEnumerator {
    /** Unique table names observed so far. */
    private final List<String> tableNames = new ArrayList<>();

    /** Join conditions. */
    private final Set<JoinConditionKey> conditions = new HashSet<>();

    /** Cached digests for the given set of inputs. */
    private final Map<String, TreeSet<Integer>> digestToInputs = new ConcurrentHashMap<>();

    /** Whether the given join graph is connected. */
    private final Map<JoinKey, Boolean> connected = new ConcurrentHashMap<>();

    /**
     * Count cross-product free join orders for the submitted join graph.
     */
    public long count() {
        if (inputCount() == 1) {
            return 1;
        }

        // Clear the state.
        digestToInputs.clear();
        connected.clear();

        // Generate leaf orders.
        List<List<Integer>> orders = generateLeafOrders();
        assert orders.size() == JoinEnumeratorUtils.fact(inputCount());

        // Generate associations.
        Set<Join> templates = generateJoinTemplates();
        assert templates.size() == JoinEnumeratorUtils.catalan(inputCount() - 1);

        // Combine leaf orders and associations.
        AtomicLong counter = new AtomicLong();
        CountDownLatch doneLatch = new CountDownLatch(orders.size() * templates.size());
        for (List<Integer> order : orders) {
            for (Join template : templates) {
                checkConnectedAsync(order, template, counter, doneLatch);
            }
        }

        // Await completion.
        try {
            doneLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted", e);
        }

        return counter.get();
    }

    /**
     * Add join condition between two tables.
     */
    public void addJoinCondition(String table1, String table2) {
        conditions.add(new JoinConditionKey(tableOrdinal(table1), tableOrdinal(table2)));
    }

    /**
     * Map unique table name to ordinal.
     */
    private int tableOrdinal(String name) {
        int index = tableNames.indexOf(name);
        if (index == -1) {
            index = tableNames.size();
            tableNames.add(name);
        }
        return index;
    }

    /**
     * Number of inputs.
     */
    private int inputCount() {
        return tableNames.size();
    }

    /**
     * Increment the counter if the given join order forms a connected graph.
     */
    private void checkConnectedAsync(
        List<Integer> order,
        Join template,
        AtomicLong counter,
        CountDownLatch doneLatch
    ) {
        ForkJoinPool.commonPool().execute(() -> {
            Join join = associate(template, order);
            ConnectedJoinShuttle shuttle = new ConnectedJoinShuttle();
            join.accept(shuttle);
            if (shuttle.connected) {
                counter.incrementAndGet();
            }
            doneLatch.countDown();
        });
    }

    /**
     * Whether there is a join condition between two nodes.
     */
    private boolean hasJoinCondition(Node left, Node right) {
        String leftDigest = left.toString();
        String rightDigest = right.toString();
        JoinKey key = new JoinKey(leftDigest, rightDigest);
        Boolean res = connected.get(key);
        if (res != null) {
            return res;
        }

        Set<Integer> leftInputs = collectInputs(leftDigest, left);
        Set<Integer> rightInputs = collectInputs(rightDigest, right);
        for (int leftInput : leftInputs) {
            for (int rightInput : rightInputs) {
                if (hasJoinCondition(leftInput, rightInput)) {
                    connected.put(key, true);
                    return true;
                }
            }
        }

        connected.put(key, false);
        return false;
    }

    /**
     * Whether there is a join condition between two tables. If not, the join of two inputs is a cross-product.
     */
    private boolean hasJoinCondition(int key1, int key2) {
        return conditions.contains(new JoinConditionKey(key1, key2));
    }

    /**
     * Replace the template with the given order of leaves.
     */
    private static Join associate(Join template, List<Integer> order) {
        return (Join)template.accept(new Shuttle() {
            @Override
            public Node visitLeaf(Leaf leaf) {
                return new Leaf(order.get(leaf.index));
            }
            @Override
            public Node visitJoin(Join join) {
                return join;
            }
        });
    }

    /**
     * Collect all inputs present in the given node. For example, A join (B join C) contains three inputs: A, B, and C.
     */
    private TreeSet<Integer> collectInputs(String digest, Node node) {
        TreeSet<Integer> res = digestToInputs.get(digest);
        if (res == null) {
            TreeSet<Integer> res0 = new TreeSet<>();
            node.accept(new Shuttle() {
                @Override
                public Node visitLeaf(Leaf leaf) {
                    res0.add(leaf.index);
                    return leaf;
                }

                @Override
                public Node visitJoin(Join join) {
                    return join;
                }
            });
            digestToInputs.put(digest, res0);
            res = res0;
        }
        return res;
    }

    /**
     * Generate all possible orders of leaves. For example, for inputs A, B, and C possible orders are: ABC, ACB,
     * BAC, BCA, CAB, CBA.
     */
    private List<List<Integer>> generateLeafOrders() {
        List<List<Integer>> ress = new ArrayList<>();
        List<Integer> currentOrder = new ArrayList<>(inputCount());
        generateLeafOrders(currentOrder, ress);
        return ress;
    }

    private void generateLeafOrders(List<Integer> currentOrder, List<List<Integer>> ress) {
        if (currentOrder.size() == inputCount()) {
            ress.add(new ArrayList<>(currentOrder));
            return;
        }

        for (int i = 0; i < inputCount(); i++) {
            if (currentOrder.contains(i)) {
                continue;
            }
            currentOrder.add(i);
            generateLeafOrders(currentOrder, ress);
            currentOrder.remove(currentOrder.size() - 1);
        }
    }

    /**
     * Generate possible associations of inputs. For example, given the inputs T1, T2, and T3 in that order, the
     * possible associations are (T1xT2)xT3 and T1x(T2xT3).
     */
    private Set<Join> generateJoinTemplates() {
        Set<Join> ress = new HashSet<>();
        List<Node> nodes = new ArrayList<>(inputCount());
        for (int i = 0; i < inputCount(); i++) {
            nodes.add(new Leaf(i));
        }
        generateJoinTemplates(nodes, ress);
        return ress;
    }

    private static void generateJoinTemplates(List<Node> nodes, Set<Join> ress) {
        if (nodes.size() == 1) {
            Node join = nodes.get(0);
            assert join instanceof Join;
            ress.add((Join)join);
            return;
        }

        int joinCount = nodes.size() - 1;
        for (int i = 0; i < joinCount; i++) {
            Node left = nodes.remove(i);
            Node right = nodes.remove(i);
            Join join = new Join(left, right);
            nodes.add(i, join);

            generateJoinTemplates(nodes, ress);

            Node removedJoin = nodes.remove(i);
            assert join == removedJoin;
            nodes.add(i, right);
            nodes.add(i, left);
        }
    }

    /**
     * Node that represents either a leaf input or a join.
     */
    private static abstract class Node {
        public abstract Node accept(Shuttle shuttle);
    }

    /**
     * Leaf input.
     */
    private static class Leaf extends Node {
        private final int index;
        private Leaf(int index) {
            this.index = index;
        }
        @Override
        public Node accept(Shuttle shuttle) {
            return shuttle.visitLeaf(this);
        }
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Leaf leaf = (Leaf) o;
            return index == leaf.index;
        }
        @Override
        public int hashCode() {
            return index;
        }
        @Override
        public String toString() {
            return Integer.toString(index);
        }
    }

    /**
     * Join of two inputs.
     */
    private static class Join extends Node {
        private final Node left;
        private final Node right;
        public Join(Node left, Node right) {
            this.left = left;
            this.right = right;
        }
        @Override
        public Node accept(Shuttle shuttle) {
            Node newLeft = left.accept(shuttle);
            Node newRight = right.accept(shuttle);
            return shuttle.visitJoin(new Join(newLeft, newRight));
        }
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Join join = (Join) o;
            if (!left.equals(join.left)) return false;
            return right.equals(join.right);
        }
        @Override
        public int hashCode() {
            int result = left.hashCode();
            result = 31 * result + right.hashCode();
            return result;
        }
        @Override
        public String toString() {
            return "(" + left + "x" + right + ")";
        }
    }

    /**
     * A visitor that can traverse the Node tree bottom-up and construct the new tree.
     */
    private interface Shuttle {
        Node visitLeaf(Leaf leaf);
        Node visitJoin(Join join);
    }

    /**
     * Visitor that checks whether all Join nodes in the tree have join conditions.
     */
    private class ConnectedJoinShuttle implements Shuttle {
        private boolean connected = true;
        @Override
        public Node visitLeaf(Leaf leaf) {
            return leaf;
        }
        @Override
        public Node visitJoin(Join join) {
            if (connected && !hasJoinCondition(join.left, join.right)) {
                connected = false;
            }
            return join;
        }
    }

    /**
     * A key for the join condition.
     */
    private static class JoinConditionKey {
        private final int first;
        private final int second;
        private JoinConditionKey(int first, int second) {
            if (first > second) {
                this.first = second;
                this.second = first;
            } else {
                this.first = first;
                this.second = second;
            }
        }
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            JoinConditionKey that = (JoinConditionKey) o;
            if (first != that.first) return false;
            return second == that.second;
        }
        @Override
        public int hashCode() {
            int result = first;
            result = 31 * result + second;
            return result;
        }
    }

    /**
     * A key that uniquely identifies a join of two inputs.
     */
    private static class JoinKey {
        private final String leftDigest;
        private final String rightDigest;
        private JoinKey(String leftDigest, String rightDigest) {
            this.leftDigest = leftDigest;
            this.rightDigest = rightDigest;
        }
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            JoinKey joinKey = (JoinKey) o;
            if (!leftDigest.equals(joinKey.leftDigest)) return false;
            return rightDigest.equals(joinKey.rightDigest);
        }
        @Override
        public int hashCode() {
            int result = leftDigest.hashCode();
            result = 31 * result + rightDigest.hashCode();
            return result;
        }
    }
}
