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
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;

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
            String filename = convertFilename(prefix, resource.getFilename());
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

    private String convertFilename(String prefix, String filename) {
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

    private String unConvertFilename(String filename, String prefix){
        return filename.replaceFirst("V", prefix);
    }

    protected A1SqlMigrationResolver.MigrationInfo getMaxBaselineMigrationInfo(String[] suffixes) {
        ResourceNameParser resourceNameParser = new ResourceNameParser(configuration);

        Map<MigrationVersion, String> migrations = resourceProvider.getResources(BASELINE_MIGRATION_PREFIX, suffixes).stream()
            .map(lr -> convertFilename(BASELINE_MIGRATION_PREFIX, lr.getFilename()))
            .map(resourceNameParser::parse)
            .collect(toMap(ResourceName::getVersion, ResourceName::getFilename));

        if (migrations.size() > 0) {
            List<MigrationVersion> versions = migrations.keySet().stream()
                .sorted()
                .collect(Collectors.toList());

            MigrationVersion version = versions.get(versions.size() - 1);
            String script = unConvertFilename(migrations.get(version), BASELINE_MIGRATION_PREFIX);

            return new MigrationInfo(version, script);
        } else {
            return new MigrationInfo();
        }
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

    protected static class MigrationInfo {
        private final MigrationVersion migration;
        private final String script;

        public MigrationInfo(MigrationVersion migration, String script) {
            this.migration = migration;
            this.script = script;
        }

        public MigrationInfo(String version, String script) {
            this(MigrationVersion.fromVersion(version), script);
        }

        public MigrationInfo() {
            this.migration = null;
            this.script = null;
        }

        public Optional<MigrationVersion> getMigration() {
            return Optional.ofNullable(migration);
        }

        public String getScript() {
            return script;
        }
    }

}
