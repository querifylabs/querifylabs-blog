package com.querifylabs.blog.trait;

import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.HashMap;
import java.util.Map;

public class Schema extends AbstractSchema {

    private final String schemaName;
    private final Map<String, org.apache.calcite.schema.Table> tableMap;

    private Schema(String schemaName, Map<String, org.apache.calcite.schema.Table> tableMap) {
        this.schemaName = schemaName;
        this.tableMap = tableMap;
    }

    public String getSchemaName() {
        return schemaName;
    }

    @Override
    public Map<String, org.apache.calcite.schema.Table> getTableMap() {
        return tableMap;
    }

    @Override
    public org.apache.calcite.schema.Schema snapshot(SchemaVersion version) {
        return this;
    }

    public static Builder newBuilder(String schemaName) {
        return new Builder(schemaName);
    }

    public static final class Builder {

        private final String schemaName;
        private final Map<String, org.apache.calcite.schema.Table> tableMap = new HashMap<>();

        private Builder(String schemaName) {
            if (schemaName == null || schemaName.isEmpty()) {
                throw new IllegalArgumentException("Schema name cannot be null or empty");
            }

            this.schemaName = schemaName;
        }

        public Builder addTable(Table table) {
            if (tableMap.containsKey(table.getTableName())) {
                throw new IllegalArgumentException("Table already defined: " + table.getTableName());
            }

            tableMap.put(table.getTableName(), table);

            return this;
        }

        public Schema build() {
            return new Schema(schemaName, tableMap);
        }
    }
}
