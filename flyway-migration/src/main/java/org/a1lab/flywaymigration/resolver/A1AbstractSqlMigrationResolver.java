package org.a1lab.flywaymigration.resolver;

import org.flywaydb.core.api.CoreMigrationType;
import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.ResourceProvider;
import org.flywaydb.core.api.callback.Event;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.api.resolver.MigrationResolver;
import org.flywaydb.core.api.resolver.ResolvedMigration;
import org.flywaydb.core.api.resource.LoadableResource;
import org.flywaydb.core.internal.resolver.ChecksumCalculator;
import org.flywaydb.core.internal.resolver.ResolvedMigrationImpl;
import org.flywaydb.core.internal.resolver.sql.SqlMigrationExecutor;
import org.flywaydb.core.internal.resource.ResourceName;
import org.flywaydb.core.internal.resource.ResourceNameParser;
import org.flywaydb.core.internal.sqlscript.SqlScript;
import org.flywaydb.core.internal.sqlscript.SqlScriptExecutorFactory;
import org.flywaydb.core.internal.sqlscript.SqlScriptFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.stream.Collectors.toList;

public abstract class A1AbstractSqlMigrationResolver implements MigrationResolver {

    protected static final String BASELINE_MIGRATION_PREFIX = "B";
    protected static final String TEST_MIGRATION_PREFIX = "T";

    protected Configuration configuration;
    protected ResourceProvider resourceProvider;
    protected SqlScriptFactory sqlScriptFactory;
    protected SqlScriptExecutorFactory sqlScriptExecutorFactory;

    protected void initResolver(Context context) {
        this.configuration = context.configuration;
        this.resourceProvider = context.resourceProvider;
        this.sqlScriptFactory = context.sqlScriptFactory;
        this.sqlScriptExecutorFactory = context.sqlScriptExecutorFactory;
    }

    protected void addMigrations(List<ResolvedMigration> migrations, String prefix, String[] suffixes, MigrationType migrationType) {
        ResourceNameParser resourceNameParser = new ResourceNameParser(configuration);

        for (LoadableResource resource : resourceProvider.getResources(prefix, suffixes)) {
            String filename = getFilename(prefix, resource.getFilename());
            ResourceName resourceName = resourceNameParser.parse(filename);

            if (!resourceName.isValid() || isSqlCallback(resourceName)) {
                continue;
            }

            SqlScript sqlScript = sqlScriptFactory.createSqlScript(resource, configuration.isMixed(), resourceProvider);

            List<LoadableResource> resources = new ArrayList<>();
            resources.add(resource);

            boolean repeatable = (migrationType != MigrationType.VERSIONED) && (migrationType != MigrationType.BASELINE);
            Integer checksum = getChecksumForLoadableResource(resources);
            Integer equivalentChecksum = getEquivalentChecksumForLoadableResource(repeatable, resources);

            migrations.add(new ResolvedMigrationImpl(
                resourceName.getVersion(),
                resourceName.getDescription(),
                resource.getRelativePath(),
                checksum,
                equivalentChecksum,
                CoreMigrationType.SQL,
                resource.getAbsolutePathOnDisk(),
                new SqlMigrationExecutor(sqlScriptExecutorFactory, sqlScript, false, false))
            );
        }
    }

    private String getFilename(String prefix, String filename) {
        // flyway community addition does not support baseline migrations and "B" prefix.
        // resourceNameParser::parse will not parse filenames starting with "B"
        // so, we do a trick, pretend to be a regular versioned migration
        switch (prefix) {
            case BASELINE_MIGRATION_PREFIX:
                return filename.replaceFirst("B", "V");
            case TEST_MIGRATION_PREFIX:
                return filename.replaceFirst("T", "V");
            default:
                return filename;
        }
    }

    protected Optional<MigrationVersion> getMaxMigrationVersion(String prefix, String[] suffixes) {
        ResourceNameParser resourceNameParser = new ResourceNameParser(configuration);

        List<MigrationVersion> versions = resourceProvider.getResources(prefix, suffixes).stream()
            .map(lr -> getFilename(prefix, lr.getFilename()))
            .map(resourceNameParser::parse)
            .map(ResourceName::getVersion)
            .sorted()
            .collect(toList());

        return (versions.size() > 0) ? Optional.of(versions.get(versions.size() - 1)) : Optional.empty();
    }

    private static boolean isSqlCallback(ResourceName result) {
        return Event.fromId(result.getPrefix()) != null;
    }

    private Integer getChecksumForLoadableResource(List<LoadableResource> loadableResources) {
        return ChecksumCalculator.calculate(loadableResources.toArray(new LoadableResource[0]));
    }

    private Integer getEquivalentChecksumForLoadableResource(boolean repeatable, List<LoadableResource> loadableResources) {
        if (repeatable) {
            return ChecksumCalculator.calculate(loadableResources.toArray(new LoadableResource[0]));
        }

        return null;
    }
}
