package org.a1lab.flywaymigration.resolver;

import org.flywaydb.core.api.MigrationType;
import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.ResourceProvider;
import org.flywaydb.core.api.callback.Event;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.api.logging.Log;
import org.flywaydb.core.api.logging.LogFactory;
import org.flywaydb.core.api.migration.JavaMigration;
import org.flywaydb.core.api.resolver.Context;
import org.flywaydb.core.api.resolver.MigrationResolver;
import org.flywaydb.core.api.resolver.ResolvedMigration;
import org.flywaydb.core.api.resource.LoadableResource;
import org.flywaydb.core.internal.database.DatabaseType;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.parser.ParsingContext;
import org.flywaydb.core.internal.resolver.ChecksumCalculator;
import org.flywaydb.core.internal.resolver.ResolvedMigrationImpl;
import org.flywaydb.core.internal.resource.ResourceName;
import org.flywaydb.core.internal.resource.ResourceNameParser;
import org.flywaydb.core.internal.scanner.LocationScannerCache;
import org.flywaydb.core.internal.scanner.ResourceNameCache;
import org.flywaydb.core.internal.scanner.Scanner;
import org.flywaydb.core.internal.sqlscript.SqlScript;
import org.flywaydb.core.internal.sqlscript.SqlScriptExecutorFactory;
import org.flywaydb.core.internal.sqlscript.SqlScriptFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static java.util.stream.Collectors.toList;

//flyway-core 8.0.5
public abstract class A1AbstractSqlMigrationResolver_8_0_5 implements MigrationResolver {

    private static final Log LOG = LogFactory.getLog(A1AbstractSqlMigrationResolver_8_0_5.class);

    //prefix for baseline migrations
    protected static final String BASELINE_MIGRATION_PREFIX = "B";
    //prefix for migrations to be used for adding test data. Not to be applied to production DB
    protected static final String TEST_MIGRATION_PREFIX = "T";

    protected Configuration configuration;
    protected ResourceProvider resourceProvider;
    protected SqlScriptFactory sqlScriptFactory;
    protected SqlScriptExecutorFactory sqlScriptExecutorFactory;
    private final ParsingContext parsingContext = new ParsingContext();

    protected void initResolver(Context context) {
        LOG.debug("init custom migration resolver");

        // This is available since flyway 9.10.
        //this.configuration = context.configuration;
        //this.resourceProvider = context.resourceProvider;
        //this.sqlScriptFactory = context.sqlScriptFactory;
        //this.sqlScriptExecutorFactory = context.sqlScriptExecutorFactory;

        this.configuration = context.getConfiguration();

        this.resourceProvider = new Scanner<>(JavaMigration.class,
            Arrays.asList(this.configuration.getLocations()), this.configuration.getClassLoader(),
            this.configuration.getEncoding(), this.configuration.isDetectEncoding(),
            false, new ResourceNameCache(), new LocationScannerCache(), this.configuration.isFailOnMissingLocations());

        JdbcConnectionFactory jdbcConnectionFactory = new JdbcConnectionFactory(this.configuration.getDataSource(), this.configuration, null);
        DatabaseType databaseType = jdbcConnectionFactory.getDatabaseType();
        this.sqlScriptFactory = databaseType.createSqlScriptFactory(this.configuration, parsingContext);
        this.sqlScriptExecutorFactory = databaseType.createSqlScriptExecutorFactory(jdbcConnectionFactory, null, null);
    }

    protected void addMigrations(List<ResolvedMigration> migrations, String prefix, String[] suffixes, A1MigrationType migrationType) {
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

            boolean repeatable = (migrationType != A1MigrationType.VERSIONED) && (migrationType != A1MigrationType.BASELINE);
            Integer checksum = getChecksumForLoadableResource(resources);
            Integer equivalentChecksum = getEquivalentChecksumForLoadableResource(repeatable, resources);

            migrations.add(new ResolvedMigrationImpl(resourceName.getVersion(), resourceName.getDescription(), resource.getRelativePath(),
                checksum, equivalentChecksum, MigrationType.SQL, resource.getAbsolutePathOnDisk(),
                new A1SqlMigrationExecutor(this.sqlScriptExecutorFactory, sqlScript, false, false)) {
                public void validate() {
                }
            });
        }
    }

    private String getFilename(String prefix, String filename) {
        // flyway community addition does not support baseline migrations (started with "B" prefix).
        // This is in particular means that resourceNameParser::parse does not parse filenames starting with "B".
        // So, we do a trick, pretend to be a regular versioned migration.
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
