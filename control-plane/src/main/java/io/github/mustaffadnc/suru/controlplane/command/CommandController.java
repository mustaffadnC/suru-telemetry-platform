package io.github.mustaffadnc.suru.controlplane.command;

import io.github.mustaffadnc.suru.controlplane.audit.AuditEntry;
import io.github.mustaffadnc.suru.controlplane.audit.AuditLog;
import io.github.mustaffadnc.suru.controlplane.security.Principal;
import io.github.mustaffadnc.suru.controlplane.security.PrincipalResolver;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Issuing commands to vehicles, and reading back what happened to them. */
@RestController
@RequestMapping("/api/v1/commands")
@Tag(name = "Commands", description = "Issue commands to vehicles and track their outcome")
public final class CommandController {

    /** Topic the relay publishes commands to. */
    static final String COMMAND_TOPIC = "commands.outbound";

    /** How long a command waits for an ACK before it is declared timed out. */
    static final Duration ACK_TIMEOUT = Duration.ofSeconds(30);

    private final CommandRepository repository;
    private final AuditLog auditLog;
    private final PrincipalResolver principals;

    /**
     * Creates the controller.
     *
     * @param repository command storage
     * @param auditLog where refusals are recorded
     * @param principals resolves the caller
     */
    public CommandController(
            CommandRepository repository, AuditLog auditLog, PrincipalResolver principals) {
        this.repository = repository;
        this.auditLog = auditLog;
        this.principals = principals;
    }

    /**
     * A request to issue a command.
     *
     * @param deviceId the vehicle
     * @param type what to do
     * @param params command parameters
     * @param idempotencyKey the caller's key; reissuing with it returns the original command
     */
    public record IssueRequest(
            @NotBlank String deviceId,
            @NotNull CommandType type,
            Map<String, Double> params,
            @NotBlank String idempotencyKey) {

        /** Defaults an absent parameter map to empty. */
        public IssueRequest {
            params = params == null ? Map.of() : Map.copyOf(params);
        }
    }

    /**
     * A command as the API reports it.
     *
     * @param id the command id
     * @param deviceId the vehicle
     * @param type what was asked for
     * @param state where it stands
     * @param ackResult the vehicle's result code, or {@code null}
     * @param issuedBy who asked
     * @param createdAt when it was accepted
     */
    public record CommandView(
            UUID id,
            String deviceId,
            CommandType type,
            CommandState state,
            Integer ackResult,
            String issuedBy,
            Instant createdAt) {

        static CommandView of(Command command) {
            return new CommandView(
                    command.id(),
                    command.deviceId(),
                    command.type(),
                    command.state(),
                    command.ack().isPresent() ? command.ack().getAsInt() : null,
                    command.issuedBy(),
                    command.createdAt());
        }
    }

    /**
     * Issues a command.
     *
     * @param request what to issue
     * @param httpRequest the incoming request, for identity
     * @return the command, {@code 201} when newly created and {@code 200} when the idempotency key
     *     matched an existing one
     * @throws SQLException if the database is unavailable
     */
    @PostMapping
    @Operation(summary = "Issue a command to a vehicle")
    public ResponseEntity<CommandView> issue(
            @Valid @RequestBody IssueRequest request, HttpServletRequest httpRequest)
            throws SQLException {

        Principal principal = principals.resolve(httpRequest);

        if (!principal.mayCommand()) {
            // The refusal is recorded before the response is written. A denial nobody can find
            // afterwards is the one an incident review needs and cannot get.
            auditLog.record(
                    AuditEntry.denied(
                            principal.tenantId(),
                            principal.subject(),
                            "command.issue",
                            request.deviceId(),
                            "role does not permit issuing commands"));
            return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
        }

        CommandRepository.Issued issued =
                repository.issue(
                        // The tenant comes from the principal, never from the request body.
                        principal.tenantId(),
                        request.deviceId(),
                        request.idempotencyKey(),
                        request.type(),
                        request.params(),
                        principal.subject(),
                        COMMAND_TOPIC,
                        ACK_TIMEOUT);

        return ResponseEntity.status(issued.created() ? HttpStatus.CREATED : HttpStatus.OK)
                .body(CommandView.of(issued.command()));
    }

    /**
     * Reads one command.
     *
     * <p>Returns {@code 404} rather than {@code 403} for a command belonging to another tenant.
     * Distinguishing the two would confirm that a given id exists somewhere, which is a small leak
     * and a free one to avoid — the lookup is tenant-scoped, so the controller never learns the
     * difference either.
     *
     * @param id the command id
     * @param httpRequest the incoming request, for identity
     * @return the command, or {@code 404}
     * @throws SQLException if the database is unavailable
     */
    @GetMapping("/{id}")
    @Operation(summary = "Read one command")
    public ResponseEntity<CommandView> get(
            @org.springframework.web.bind.annotation.PathVariable UUID id,
            HttpServletRequest httpRequest)
            throws SQLException {

        Principal principal = principals.resolve(httpRequest);
        return repository
                .find(principal.tenantId(), id)
                .map(CommandView::of)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    /**
     * Lists a device's recent commands.
     *
     * <p>The device is a query parameter rather than a path segment: device ids contain a slash
     * ({@code link/sys1}), which a path segment splits and turns into a 404.
     *
     * @param deviceId the vehicle
     * @param limit how many to return
     * @param httpRequest the incoming request, for identity
     * @return the commands, newest first
     * @throws SQLException if the database is unavailable
     */
    @GetMapping
    @Operation(summary = "List recent commands for a device")
    public List<CommandView> list(
            @RequestParam String deviceId,
            @RequestParam(defaultValue = "50") int limit,
            HttpServletRequest httpRequest)
            throws SQLException {

        Principal principal = principals.resolve(httpRequest);
        return repository.findByDevice(principal.tenantId(), deviceId, Math.clamp(limit, 1, 500))
                .stream()
                .map(CommandView::of)
                .toList();
    }
}
