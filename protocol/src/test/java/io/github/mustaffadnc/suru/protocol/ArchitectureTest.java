package io.github.mustaffadnc.suru.protocol;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.library.GeneralCodingRules.NO_CLASSES_SHOULD_ACCESS_STANDARD_STREAMS;
import static com.tngtech.archunit.library.GeneralCodingRules.NO_CLASSES_SHOULD_THROW_GENERIC_EXCEPTIONS;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Architecture rules live here rather than in documentation — breaking one breaks CI.
 *
 * <p>The core ArchUnit API is used instead of the {@code archunit-junit5} engine: the engine is
 * tightly coupled to the JUnit Platform version, the core API is not.
 */
class ArchitectureTest {

    private static final String PROTOCOL_PACKAGE = "io.github.mustaffadnc.suru.protocol";

    private static JavaClasses productionClasses;

    @BeforeAll
    static void importClasses() {
        productionClasses =
                new ClassFileImporter()
                        .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                        .importPackages(PROTOCOL_PACKAGE);
    }

    @Test
    @DisplayName("The protocol module may not depend on anything outside the JDK")
    void protocolStaysDependencyFree() {
        // Isolation is this module's whole value: testable without Netty, measurable on its
        // own under JMH, portable into another project. If Netty, Kafka or Spring ever leaks
        // in here, this test breaks.
        classes()
                .should()
                .onlyDependOnClassesThat()
                .resideInAnyPackage(PROTOCOL_PACKAGE + "..", "java..")
                .because("the protocol module is deliberately dependency-free — see protocol/build.gradle.kts")
                .check(productionClasses);
    }

    @Test
    @DisplayName("Nothing on the hot path writes to the standard streams")
    void noConsoleLogging() {
        // Writing to the console per packet costs latency; logging belongs to the layer above.
        NO_CLASSES_SHOULD_ACCESS_STANDARD_STREAMS.check(productionClasses);
    }

    @Test
    @DisplayName("No generic exception types are thrown")
    void noGenericExceptions() {
        // A malformed frame and a transport failure must stay distinguishable —
        // RuntimeException/Exception cannot express that.
        NO_CLASSES_SHOULD_THROW_GENERIC_EXCEPTIONS.check(productionClasses);
    }
}
