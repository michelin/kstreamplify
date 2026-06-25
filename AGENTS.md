# Agent Instructions

## Repository Structure

- `kstreamplify-core`: core Kafka Streams functionality.
- `kstreamplify-core-test`: test utilities and base classes for testing topologies.
- `kstreamplify-spring-boot`: Spring Boot autoconfiguration and web services.

## Commands

- Build with `mvn clean package`.
- Run the tests with `mvn test`.
- Run `mvn spotless:apply` before committing to apply Palantir Java Format.

## Coding Standards

- Target Java 17.
- Never use `var`. Always declare variables with their explicit type.
- Code follows Palantir Java Format.
- Add minimal Javadoc to every method in production code (`src/main`), including `@Override` methods, with descriptions for parameters and return values. Start each description with an uppercase letter.

## Testing Standards

- Name test classes `<ClassName>Test`.
- Name unit tests with the "should..." convention (e.g. `shouldNotRegisterPropertiesWhenNull`).
- Use JUnit Jupiter assertions (`org.junit.jupiter.api.Assertions`).
- Use Mockito with `@ExtendWith(MockitoExtension.class)`.
- Use Testcontainers for Kafka integration tests.