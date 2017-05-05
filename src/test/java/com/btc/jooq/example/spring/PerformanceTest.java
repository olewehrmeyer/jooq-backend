package com.btc.jooq.example.spring;

import ch.qos.logback.classic.Level;
import org.jooq.*;

import static org.jooq.example.db.Tables.*;

import org.jooq.example.db.tables.Author;
import org.jooq.example.db.tables.Book;
import org.jooq.example.spring.Application;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = Application.class)
public class PerformanceTest {

    Logger LOG = LoggerFactory.getLogger(PerformanceTest.class);

    @Autowired
    DSLContext create;

    @Autowired
    DataSource dataSource;

    @Test
    @Ignore
    public void insertDataWithJooqSeveralQueries() throws Exception {

        ((ch.qos.logback.classic.Logger)LOG).getLoggerContext().getLogger("org.jooq").setLevel(Level.WARN);
        Book b = BOOK;
        Author a = AUTHOR;

        LOG.info("Deleting tables...");

        create.deleteFrom(b).execute();
        create.deleteFrom(a).execute();

        LOG.info("Creating author...");

        create.insertInto(a).columns(a.ID, a.LAST_NAME).values(42, "BTC").execute();

        List<Query> inserts = new LinkedList<>();
        LOG.info("Starting time measurement...");
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < 1_000_000; i++) {
            inserts.add(create.insertInto(b)
                    .columns(b.ID, b.TITLE, b.AUTHOR_ID)
                    .values(i, "BTC Hackathon Teil "+ i, 42));
        }
        create.batch(inserts).execute();
        create.execute("COMMIT");

        long duration = System.currentTimeMillis() - startTime;
        LOG.info("Took {} ms ({} s) to insert the data", duration, duration / 1000);

    }

    @Test
    @Ignore
    public void insertDataWithJooqBind() throws Exception {

        ((ch.qos.logback.classic.Logger)LOG).getLoggerContext().getLogger("org.jooq").setLevel(Level.WARN);
        Book b = BOOK;
        Author a = AUTHOR;

        LOG.info("Deleting tables...");

        create.deleteFrom(b).execute();
        create.deleteFrom(a).execute();

        LOG.info("Creating author...");

        create.insertInto(a).columns(a.ID, a.LAST_NAME).values(42, "BTC").execute();

        BatchBindStep batch;
        LOG.info("Starting time measurement...");
        long startTime = System.currentTimeMillis();
        batch = create.batch(create.insertInto(b)
                .columns(b.ID, b.TITLE, b.AUTHOR_ID)
                .values((Integer)null, null, null));
        for (int i = 0; i < 1_000_000; i++) {
            batch.bind(i, "BTC Hackathon " + i, 42); //nicht mehr typsicher
        }
        batch.execute();
        create.execute("COMMIT");

        long duration = System.currentTimeMillis() - startTime;
        LOG.info("Took {} ms ({} s) to insert the data", duration, duration / 1000);

    }

    @Test
    @Ignore
    public void insertDataWithStatement() throws Exception {
        final Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();

        statement.execute("DELETE FROM public.book");
        statement.execute("DELETE FROM public.author");

        LOG.info("Creating author...");

        statement.execute("INSERT INTO PUBLIC.author (id, last_name) VALUES (42, 'BTC')");
        LOG.info("Starting time measurement...");
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < 1_000_000; i++) {
            statement.addBatch("INSERT INTO public.book (id, title, author_id) VALUES (" + i + ", 'BTC Hackathon " + i + "', 42)");
        }
        statement.executeBatch();
        statement.execute("COMMIT");
        long duration = System.currentTimeMillis() - startTime;
        LOG.info("Took {} ms ({} s) to insert the data", duration, duration / 1000);
    }

    @Test
    public void insertDataWithPreparedStatement() throws Exception {
        final Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();

        statement.execute("DELETE FROM public.book");
        statement.execute("DELETE FROM public.author");

        LOG.info("Creating author...");

        statement.execute("INSERT INTO PUBLIC.author (id, last_name) VALUES (42, 'BTC')");

        PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO public.book (id, title, author_id) VALUES (?, ?, ?)");
        LOG.info("Starting time measurement...");
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < 1_000_000; i++) {
            preparedStatement.setInt(1, i);
            preparedStatement.setString(2, "BTC Hackathon " + i);
            preparedStatement.setInt(3, 42);
            preparedStatement.addBatch();
        }
        preparedStatement.executeBatch();
        statement.execute("COMMIT");
        long duration = System.currentTimeMillis() - startTime;
        LOG.info("Took {} ms ({} s) to insert the data", duration, duration / 1000);
    }
}
