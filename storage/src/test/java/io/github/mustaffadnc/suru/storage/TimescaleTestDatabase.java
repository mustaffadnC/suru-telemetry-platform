package io.github.mustaffadnc.suru.storage;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
// org.testcontainers.containers.PostgreSQLContainer is deprecated in Testcontainers 2.x —
// the same module reorganisation that renamed the artifacts moved the classes into
// per-database packages. -Werror caught the old import.
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * A TimescaleDB container plus a pooled data source, shared by the storage tests.
 *
 * <p>Testcontainers' JUnit extension is avoided here for the same reason as elsewhere in this
 * project: it binds to the JUnit Platform version and this build is on Platform 6.
 */
public final class TimescaleTestDatabase implements AutoCloseable {

    private static final DockerImageName IMAGE =
            DockerImageName.parse("timescale/timescaledb:latest-pg17")
                    // Testcontainers recognises the container as PostgreSQL by image name; the
                    // Timescale image is Postgres with an extension, so it has to be declared
                    // compatible explicitly.
                    .asCompatibleSubstituteFor("postgres");

    private final PostgreSQLContainer container;
    private final HikariDataSource dataSource;

    private TimescaleTestDatabase(PostgreSQLContainer container, HikariDataSource dataSource) {
        this.container = container;
        this.dataSource = dataSource;
    }

    /**
     * Starts a container and migrates it to the current schema.
     *
     * @return a ready database
     */
    public static TimescaleTestDatabase startAndMigrate() {
        PostgreSQLContainer container =
                new PostgreSQLContainer(IMAGE)
                        .withDatabaseName("suru")
                        .withUsername("suru")
                        .withPassword("suru_test_only")
                        .withCommand("postgres", "-c", "shared_preload_libraries=timescaledb");
        container.start();

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(container.getJdbcUrl());
        config.setUsername(container.getUsername());
        config.setPassword(container.getPassword());
        config.setMaximumPoolSize(8);
        HikariDataSource dataSource = new HikariDataSource(config);

        TelemetrySchema.migrate(dataSource);
        return new TimescaleTestDatabase(container, dataSource);
    }

    /**
     * The pooled data source.
     *
     * @return the data source
     */
    public DataSource dataSource() {
        return dataSource;
    }

    /**
     * Runs a query returning a single value.
     *
     * @param sql the query
     * @return the first column of the first row, as a string, or {@code null} if there are no rows
     * @throws SQLException if the query fails
     */
    public String queryOne(String sql) throws SQLException {
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    /**
     * Runs a query returning the first column of every row.
     *
     * @param sql the query
     * @return the values
     * @throws SQLException if the query fails
     */
    public List<String> queryColumn(String sql) throws SQLException {
        List<String> values = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                values.add(rs.getString(1));
            }
        }
        return values;
    }

    /**
     * Executes a statement.
     *
     * @param sql the statement
     * @throws SQLException if it fails
     */
    public void execute(String sql) throws SQLException {
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    @Override
    public void close() {
        dataSource.close();
        container.stop();
    }
}
