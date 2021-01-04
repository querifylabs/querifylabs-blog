package com.querifylabs.blog.trait;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;

import static org.junit.Assert.assertSame;

public class TraitTest {

    private static final String TABLE_PARTITIONED = "partitioned";
    private static final String TABLE_SINGLETON = "singleton";

    @Test
    public void testEnforceSingletonOnPartitioned() {
        enforceSingleton(TABLE_PARTITIONED);
    }

    @Test
    public void testEnforceSingletonOnSingleton() {
        enforceSingleton(TABLE_SINGLETON);
    }

    private static void enforceSingleton(String tableName) {
        // Prepare supporting objects.
        Prepare.CatalogReader schema = createSchema();
        VolcanoPlanner planner = createPlanner();

        // Create a table scan on the desired table.
        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(schema.getTypeFactory()));
        RelBuilderFactory factory = RelBuilder.proto(RelFactories.DEFAULT_TABLE_SCAN_FACTORY);
        RelBuilder relBuilder = factory.create(cluster, schema);
        RelNode node = relBuilder.scan(tableName).build();
        print("BEFORE", node);

        // Use the built-in rule that will expand abstract converters.
        RuleSet rules = RuleSets.ofList(AbstractConverter.ExpandConversionRule.INSTANCE);

        // Prepare the desired traits with the SINGLETON distribution.
        RelTraitSet desiredTraits = node.getTraitSet().plus(Distribution.SINGLETON);

        // Use the planner to enforce the desired traits.
        RelNode optimizedNode = Programs.of(rules).run(
            planner,
            node,
            desiredTraits,
            Collections.emptyList(),
            Collections.emptyList()
        );

        print("AFTER", optimizedNode);

        assertSame(Distribution.SINGLETON, optimizedNode.getTraitSet().getTrait(DistributionTraitDef.INSTANCE));
    }

    private static Prepare.CatalogReader createSchema() {
        // Table with PARTITIONED distribution.
        Table table1 = Table.newBuilder(TABLE_PARTITIONED, Distribution.PARTITIONED)
            .addField("field", SqlTypeName.DECIMAL).build();

        // Table with SINGLETON distribution.
        Table table2 = Table.newBuilder(TABLE_SINGLETON, Distribution.SINGLETON)
            .addField("field", SqlTypeName.DECIMAL).build();

        Schema schema = Schema.newBuilder("schema").addTable(table1).addTable(table2).build();

        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

        CalciteConnectionConfig config = CalciteConnectionConfig.DEFAULT;

        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false);
        rootSchema.add(schema.getSchemaName(), schema);

        return new CalciteCatalogReader(
            rootSchema,
            Collections.singletonList(schema.getSchemaName()),
            typeFactory,
            config
        );
    }

    private static VolcanoPlanner createPlanner() {
        VolcanoPlanner planner = new VolcanoPlanner();

        // Register distribution trait.
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(DistributionTraitDef.INSTANCE);

        // DO NOT USE IN PRODUCTION: a quirk to allow Apache Calcite calculate costs for logical nodes.
        // Without this line we would have to use a custom convention, that makes the example more complex.
        planner.setNoneConventionHasInfiniteCost(false);

        return planner;
    }

    private static void print(String header, RelNode relTree) {
        StringWriter sw = new StringWriter();

        sw.append(header).append(":").append("\n");

        RelWriterImpl relWriter = new RelWriterImpl(new PrintWriter(sw), SqlExplainLevel.DIGEST_ATTRIBUTES, true);

        relTree.explain(relWriter);

        System.out.println(sw.toString());
    }
}
