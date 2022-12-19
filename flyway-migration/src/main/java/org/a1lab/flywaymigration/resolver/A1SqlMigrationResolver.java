package org.a1lab.flywaymigration.resolver;

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

public class A1SqlMigrationResolver extends A1AbstractSqlMigrationResolver_8_0_5 {

    private static final String MAX_MIGRATION_QUERY = "select version, script from flyway_schema_history fsh " +
        "where version = ( select max(version) from flyway_schema_history)";
    private static final String VERSION_FIELD = "version";
    private static final String SCRIPT_FIELD = "script";

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

    private MigrationInfo getMaxAppliedMigration() {
        try (Connection connection = configuration.getDataSource().getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(MAX_MIGRATION_QUERY)) {

            if (resultSet.next()) {
                String version = resultSet.getString(VERSION_FIELD);
                String script = resultSet.getString(SCRIPT_FIELD);

                return new MigrationInfo(version, script);
            }

        } catch (SQLException e) {
            return new MigrationInfo();
        }

        return new MigrationInfo();
    }

    private boolean applyBaselineMigration(String[] suffixes) {
        MigrationInfo baselineMigration = getMaxBaselineMigrationInfo(suffixes);
        MigrationInfo appliedMigration = getMaxAppliedMigration();

        if (!baselineMigration.getMigration().isPresent()) {
            return false;
        }

        return !appliedMigration.getMigration().isPresent() ||
            appliedMigration.getScript().equals(baselineMigration.getScript());
    }
}