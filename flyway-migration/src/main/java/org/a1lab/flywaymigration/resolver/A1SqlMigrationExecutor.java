package org.a1lab.flywaymigration.resolver;

import org.flywaydb.core.api.executor.Context;
import org.flywaydb.core.api.executor.MigrationExecutor;
import org.flywaydb.core.internal.database.DatabaseExecutionStrategy;
import org.flywaydb.core.internal.database.DatabaseType;
import org.flywaydb.core.internal.database.DatabaseTypeRegister;
import org.flywaydb.core.internal.sqlscript.SqlScript;
import org.flywaydb.core.internal.sqlscript.SqlScriptExecutorFactory;

import java.sql.SQLException;

public class A1SqlMigrationExecutor implements MigrationExecutor {
    private final SqlScriptExecutorFactory sqlScriptExecutorFactory;
    private final SqlScript sqlScript;
    private final boolean undo;
    private final boolean batch;

    public void execute(Context context) throws SQLException {
        DatabaseType databaseType = DatabaseTypeRegister.getDatabaseTypeForConnection(context.getConnection());
        DatabaseExecutionStrategy strategy = databaseType.createExecutionStrategy(context.getConnection());
        strategy.execute(() -> {
            this.executeOnce(context);
            return true;
        });
    }

    private void executeOnce(Context context) {
        boolean outputQueryResults = false;
        this.sqlScriptExecutorFactory.createSqlScriptExecutor(context.getConnection(), this.undo, this.batch, outputQueryResults).execute(this.sqlScript);
    }

    public boolean canExecuteInTransaction() {
        return this.sqlScript.executeInTransaction();
    }

    public boolean shouldExecute() {
        return this.sqlScript.shouldExecute();
    }

    public A1SqlMigrationExecutor(SqlScriptExecutorFactory sqlScriptExecutorFactory, SqlScript sqlScript, boolean undo, boolean batch) {
        this.sqlScriptExecutorFactory = sqlScriptExecutorFactory;
        this.sqlScript = sqlScript;
        this.undo = undo;
        this.batch = batch;
    }
}
