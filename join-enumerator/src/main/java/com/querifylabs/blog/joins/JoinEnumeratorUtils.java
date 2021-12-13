package com.querifylabs.blog.joins;

public class JoinEnumeratorUtils {
    private JoinEnumeratorUtils() {}

    /**
     * Calculates factorial of n.
     */
    public static Long fact(int n) {
        if (n == 1) {
            return 1L;
        }
        return n * fact(n - 1);
    }

    /**
     * Calculates Catalan number of n.
     */
    public static Long catalan(int n) {
        return fact(2 * n) / (fact(n + 1) * fact(n));
    }
}
