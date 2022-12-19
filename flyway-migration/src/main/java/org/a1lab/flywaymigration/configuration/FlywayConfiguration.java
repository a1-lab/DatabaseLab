package org.a1lab.flywaymigration.configuration;

import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.springframework.boot.autoconfigure.flyway.FlywayConfigurationCustomizer;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FlywayConfiguration implements FlywayConfigurationCustomizer {
    @Override
    public void customize(FluentConfiguration configuration) {
        configuration.resolvers("org.a1lab.flywaymigration.resolver.A1SqlMigrationResolver");
    }
}
