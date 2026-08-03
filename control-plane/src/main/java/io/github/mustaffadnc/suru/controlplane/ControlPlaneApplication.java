package io.github.mustaffadnc.suru.controlplane;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * The operator-facing service.
 *
 * <p>Separate from the ingest gateway because the two scale on unrelated axes: this one serves a
 * modest number of human-driven requests and carries the business logic, while the gateway is
 * network-bound and grows with connection count. See ADR-0002.
 *
 * <p>Flyway is deliberately not wired in here. The schema is applied by the storage module's
 * migration runner as a deployment step, so a rolling restart of several API instances cannot have
 * two of them racing to migrate the same database.
 */
@SpringBootApplication
@OpenAPIDefinition(
        info =
                @Info(
                        title = "SÜRÜ control plane",
                        version = "v1",
                        description = "Query API over stored UAV and IoT telemetry."))
public class ControlPlaneApplication {

    /**
     * Entry point.
     *
     * @param args command-line arguments
     */
    public static void main(String[] args) {
        SpringApplication.run(ControlPlaneApplication.class, args);
    }
}
