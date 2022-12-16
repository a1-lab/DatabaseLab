package org.a1lab.flywaymigration.resolver;

import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.resolver.Context;
import org.flywaydb.core.api.resolver.ResolvedMigration;
import org.flywaydb.core.internal.resolver.ResolvedMigrationComparator;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public class A1SqlMigrationResolver extends A1AbstractSqlMigrationResolver_8_0_5 {

    private static final String MAX_MIGRATION_VERSION_QUERY = "select max(version) max_version from flyway_schema_history fsh";
    private static final String MAX_VERSION_FIELD = "max_version";

    @Override
    public Collection<ResolvedMigration> resolveMigrations(Context context) {
        initResolver(context);

        List<ResolvedMigration> migrations = new ArrayList<>();
        String[] suffixes = configuration.getSqlMigrationSuffixes();

        if (applyBaselineMigration(suffixes)) {
            //empty schema, baseline migration present, apply it
            addMigrations(migrations, BASELINE_MIGRATION_PREFIX, suffixes, A1MigrationType.BASELINE);
        } else {
            addMigrations(migrations, configuration.getSqlMigrationPrefix(), suffixes, A1MigrationType.VERSIONED);
        }

        addMigrations(migrations, configuration.getRepeatableSqlMigrationPrefix(), suffixes, A1MigrationType.REPEATABLE);

        migrations.sort(new ResolvedMigrationComparator());
        return migrations;
    }

    private Optional<MigrationVersion> getMaxAppliedVersion() {
        try (Connection connection = configuration.getDataSource().getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(MAX_MIGRATION_VERSION_QUERY)) {

            if (resultSet.next()) {
                String version = resultSet.getString(MAX_VERSION_FIELD);
                return Optional.ofNullable(MigrationVersion.fromVersion(version));
            }

        } catch (SQLException e) {
            return Optional.empty();
        }

        return Optional.empty();
    }

    private boolean applyBaselineMigration(String[] suffixes) {
        Optional<MigrationVersion> baselineMaxMigration = getMaxBaselineMigrationVersion(suffixes);
        Optional<MigrationVersion> maxVersion = getMaxAppliedVersion();

        return (!maxVersion.isPresent() && baselineMaxMigration.isPresent());
    }
}