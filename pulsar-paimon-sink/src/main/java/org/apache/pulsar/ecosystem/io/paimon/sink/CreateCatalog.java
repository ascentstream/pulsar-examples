package org.apache.pulsar.ecosystem.io.paimon.sink;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.fs.Path;

public class CreateCatalog {

    /**
     * Creates a Filesystem Catalog.
     *
     * @return the created Catalog
     */
    public static Catalog createFilesystemCatalog() {
        CatalogContext context = CatalogContext.create(new Path("/pulsar/data"));
        return CatalogFactory.createCatalog(context);
    }
}