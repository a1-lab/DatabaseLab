package org.a1lab.flaywaymigration;

import org.flywaydb.core.Flyway;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class FlaywayMigrationApplication implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(FlaywayMigrationApplication.class, args);
    }

    @Override
    public void run(String... args) {
        Flyway flyway = Flyway.configure()
            .dataSource("jdbc:postgresql://localhost/test_db", "postgres", "postgres")
            .locations("classpath:db/migration")
            .baselineOnMigrate(true)
            .skipDefaultResolvers(true)
            .load();

		flyway.migrate();
    }
}
