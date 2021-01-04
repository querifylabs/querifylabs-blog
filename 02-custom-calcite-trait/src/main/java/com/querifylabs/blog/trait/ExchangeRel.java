package com.querifylabs.blog.trait;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;

import java.util.List;

public class ExchangeRel extends SingleRel {
    public ExchangeRel(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode input
    ) {
        super(cluster, traits, input);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new ExchangeRel(getCluster(), traitSet, inputs.get(0));
    }
}
