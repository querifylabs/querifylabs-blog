package com.querifylabs.blog.trait;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;

public class DistributionTraitDef extends RelTraitDef<Distribution> {

    public static DistributionTraitDef INSTANCE = new DistributionTraitDef();

    private DistributionTraitDef() {
        // No-op.
    }

    @Override
    public Class<Distribution> getTraitClass() {
        return Distribution.class;
    }

    @Override
    public String getSimpleName() {
        return "DISTRIBUTION";
    }

    @Override
    public RelNode convert(
        RelOptPlanner planner,
        RelNode rel,
        Distribution toTrait,
        boolean allowInfiniteCostConverters
    ) {
        Distribution fromTrait = rel.getTraitSet().getTrait(DistributionTraitDef.INSTANCE);

        if (fromTrait.satisfies(toTrait)) {
            return rel;
        }

        return new ExchangeRel(
            rel.getCluster(),
            rel.getTraitSet().plus(toTrait),
            rel
        );
    }

    @Override
    public boolean canConvert(
        RelOptPlanner planner,
        Distribution fromTrait,
        Distribution toTrait
    ) {
        return true;
    }

    @Override
    public Distribution getDefault() {
        return Distribution.ANY;
    }
}
