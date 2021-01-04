package com.querifylabs.blog.trait;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

public class Table extends AbstractTable implements TranslatableTable {

    private final String tableName;
    private final Distribution distribution;
    private final List<String> fieldNames;
    private final List<SqlTypeName> fieldTypes;

    private RelDataType rowType;

    private Table(String tableName, Distribution distribution, List<String> fieldNames, List<SqlTypeName> fieldTypes) {
        this.tableName = tableName;
        this.distribution = distribution;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        if (rowType == null) {
            List<RelDataTypeField> fields = new ArrayList<>(fieldNames.size());

            for (int i = 0; i < fieldNames.size(); i++) {
                RelDataType fieldType = typeFactory.createSqlType(fieldTypes.get(i));
                RelDataTypeField field = new RelDataTypeFieldImpl(fieldNames.get(i), i, fieldType);
                fields.add(field);
            }

            rowType = new RelRecordType(StructKind.PEEK_FIELDS, fields, false);
        }

        return rowType;
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        RelTraitSet traitSet = context.getCluster().traitSetOf(distribution);

        return new LogicalTableScan(
            context.getCluster(),
            traitSet,
            context.getTableHints(),
            relOptTable
        );
    }

    public static Builder newBuilder(String tableName, Distribution distribution) {
        return new Builder(tableName, distribution);
    }

    public static final class Builder {

        private final String tableName;
        private final Distribution distribution;
        private final List<String> fieldNames = new ArrayList<>();
        private final List<SqlTypeName> fieldTypes = new ArrayList<>();

        private Builder(String tableName, Distribution distribution) {
            if (tableName == null || tableName.isEmpty()) {
                throw new IllegalArgumentException("Table name cannot be null or empty");
            }

            this.tableName = tableName;
            this.distribution = distribution;
        }

        public Builder addField(String name, SqlTypeName typeName) {
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException("Field name cannot be null or empty");
            }

            if (fieldNames.contains(name)) {
                throw new IllegalArgumentException("Field already defined: " + name);
            }

            fieldNames.add(name);
            fieldTypes.add(typeName);

            return this;
        }

        public Table build() {
            if (fieldNames.isEmpty()) {
                throw new IllegalStateException("Table must have at least one field");
            }

            return new Table(tableName, distribution, fieldNames, fieldTypes);
        }
    }
}
