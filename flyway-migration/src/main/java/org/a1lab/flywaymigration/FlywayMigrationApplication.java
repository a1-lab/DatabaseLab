package org.a1lab.flywaymigration;

import org.flywaydb.core.Flyway;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class FlywayMigrationApplication {
//public class FlywayMigrationApplication implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(FlywayMigrationApplication.class, args);
    }

//    @Override
//    public void run(String... args) {
//        Flyway flyway = Flyway.configure()
//            .dataSource("jdbc:postgresql://localhost/test_db", "postgres", "postgres")
//            .locations("classpath:db/migration")
//            .baselineOnMigrate(true)
//            .skipDefaultResolvers(true)
//            .resolvers("org.a1lab.flywaymigration.resolver.A1SqlMigrationResolver")
//            .load();
//
//		flyway.migrate();
//        System.out.println("Hello, world!");
//    }

}
