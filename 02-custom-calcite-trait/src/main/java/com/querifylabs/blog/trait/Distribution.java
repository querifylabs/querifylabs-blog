package com.querifylabs.blog.trait;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;

public class Distribution implements RelTrait {

    public static final Distribution ANY = new Distribution(Type.ANY);
    public static final Distribution PARTITIONED = new Distribution(Type.PARTITIONED);
    public static final Distribution SINGLETON = new Distribution(Type.SINGLETON);

    private final Type type;

    private Distribution(Type type) {
        this.type = type;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public RelTraitDef getTraitDef() {
        return DistributionTraitDef.INSTANCE;
    }

    @Override
    public boolean satisfies(RelTrait toTrait) {
        Distribution toTrait0 = (Distribution) toTrait;

        if (toTrait0.type == Type.ANY) {
            return true;
        }

        return this.type.equals(toTrait0.type);
    }

    @Override
    public void register(RelOptPlanner planner) {
        // No-op.
    }

    @Override
    public String toString() {
        return type.name();
    }

    enum Type {
        ANY,
        PARTITIONED,
        SINGLETON
    }
}
