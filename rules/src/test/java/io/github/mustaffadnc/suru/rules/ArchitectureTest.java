package io.github.mustaffadnc.suru.rules;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.GeneralCodingRules.NO_CLASSES_SHOULD_ACCESS_STANDARD_STREAMS;
import static com.tngtech.archunit.library.GeneralCodingRules.NO_CLASSES_SHOULD_THROW_GENERIC_EXCEPTIONS;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The rules module's isolation, enforced rather than described.
 *
 * <p>The core ArchUnit API is used instead of the {@code archunit-junit5} engine: the engine is
 * tightly coupled to the JUnit Platform version, the core API is not.
 */
class ArchitectureTest {

    private static final String RULES_PACKAGE = "io.github.mustaffadnc.suru.rules";

    private static JavaClasses productionClasses;

    @BeforeAll
    static void importClasses() {
        productionClasses =
                new ClassFileImporter()
                        .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                        .importPackages(RULES_PACKAGE);
    }

    @Test
    @DisplayName("The rules module may not depend on anything outside the JDK")
    void rulesStayDependencyFree() {
        // Same reasoning as the protocol module. The valuable part of alerting is when a
        // condition becomes an alert, and that has to be testable with a hand-written instant
        // instead of a broker and a sleep.
        classes()
                .should()
                .onlyDependOnClassesThat()
                .resideInAnyPackage(RULES_PACKAGE + "..", "java..")
                .because("the rules module is deliberately dependency-free — see rules/build.gradle.kts")
                .check(productionClasses);
    }

    @Test
    @DisplayName("Nothing in the rules module reads the clock")
    void noAmbientClock() {
        // Every instant arrives through Observation. A rule that called Instant.now() would be
        // untestable at its boundaries — the debounce edges are the whole point of this module,
        // and a test for them would have to sleep and hope. It would also evaluate historical
        // data against the wall clock during replay, firing telemetry-loss alerts for devices
        // whose records simply had not been processed yet.
        noClasses()
                .should()
                .callMethod(java.time.Instant.class, "now")
                .orShould()
                .callMethod(System.class, "currentTimeMillis")
                .orShould()
                .callMethod(System.class, "nanoTime")
                .because("time enters this module only through Observation.at()")
                .check(productionClasses);
    }

    @Test
    @DisplayName("Nothing writes to the standard streams")
    void noConsoleLogging() {
        NO_CLASSES_SHOULD_ACCESS_STANDARD_STREAMS.check(productionClasses);
    }

    @Test
    @DisplayName("No generic exception types are thrown")
    void noGenericExceptions() {
        NO_CLASSES_SHOULD_THROW_GENERIC_EXCEPTIONS.check(productionClasses);
    }
}
