package org.apache.pulsar.ecosystem.io.paimon.sink;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataTypes;

public class GetTable {
    public static Table getTable() {
        Identifier identifier = Identifier.create("pulsar_paimon", "message_trace");
        try {
            Catalog catalog = CreateCatalog.createFilesystemCatalog();
            org.apache.paimon.schema.Schema.Builder schemaBuilder = org.apache.paimon.schema.Schema.newBuilder();
            schemaBuilder.primaryKey("f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9");
            schemaBuilder.partitionKeys("f0");

            schemaBuilder.column("f0", DataTypes.STRING());
            schemaBuilder.column("f1", DataTypes.STRING());
            schemaBuilder.column("f2", DataTypes.STRING());
            schemaBuilder.column("f3", DataTypes.STRING());
            schemaBuilder.column("f4", DataTypes.STRING());
            schemaBuilder.column("f5", DataTypes.STRING());
            schemaBuilder.column("f6", DataTypes.STRING());
            schemaBuilder.column("f7", DataTypes.STRING());
            schemaBuilder.column("f8", DataTypes.STRING());
            schemaBuilder.column("f9", DataTypes.STRING());
            Schema schema = schemaBuilder.build();

            catalog.createDatabase("pulsar_paimon", true);
            catalog.createTable(identifier, schema, true);
            return catalog.getTable(identifier);
        } catch (Catalog.TableNotExistException e) {
            // do something
            throw new RuntimeException("table not exist");
        } catch (Catalog.TableAlreadyExistException e) {
            throw new RuntimeException(e);
        } catch (Catalog.DatabaseNotExistException e) {
            throw new RuntimeException(e);
        } catch (Catalog.DatabaseAlreadyExistException e) {
            throw new RuntimeException(e);
        }
    }
}