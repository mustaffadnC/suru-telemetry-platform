package io.github.mustaffadnc.suru.storage;

import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.output.MigrateResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Applies the telemetry schema.
 *
 * <p>Migrations live in {@code db/migration} and are applied by Flyway rather than by hand or at
 * application startup guesswork: the schema a query was written against has to be knowable, and
 * "whatever the last person ran" is not a schema version.
 *
 * <p>Two of the migrations disable Flyway's transaction wrapper through a sibling {@code .conf}
 * file. TimescaleDB's continuous aggregates and its policy functions cannot be created inside a
 * transaction block — the failure is a runtime error deep in the migration rather than anything
 * Flyway can anticipate, so the exemption is declared per script.
 */
public final class TelemetrySchema {

    private static final Logger log = LoggerFactory.getLogger(TelemetrySchema.class);

    private TelemetrySchema() {
        throw new AssertionError("utility class");
    }

    /**
     * Migrates a database to the current schema version.
     *
     * @param dataSource the target database
     * @return how many migrations were applied
     */
    public static int migrate(DataSource dataSource) {
        Flyway flyway =
                Flyway.configure()
                        .dataSource(dataSource)
                        .locations("classpath:db/migration")
                        // The hypertable and its policies are TimescaleDB-specific, so a clean
                        // rebuild is the only supported way back — out-of-order or repaired
                        // migrations would leave policies attached to tables that no longer
                        // match them.
                        .outOfOrder(false)
                        .validateMigrationNaming(true)
                        .load();

        MigrateResult result = flyway.migrate();
        log.info(
                "schema at version {} ({} migration(s) applied)",
                result.targetSchemaVersion,
                result.migrationsExecuted);
        return result.migrationsExecuted;
    }
}
