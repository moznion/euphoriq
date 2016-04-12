package net.moznion.euphoriq.jobbroker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Data;
import lombok.EqualsAndHashCode;
import me.geso.tinyorm.Row;
import me.geso.tinyorm.TinyORM;
import me.geso.tinyorm.annotations.Column;
import me.geso.tinyorm.annotations.PrimaryKey;
import me.geso.tinyorm.annotations.Table;
import net.moznion.euphoriq.Job;
import net.moznion.euphoriq.exception.JobCanceledException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Optional;
import java.util.OptionalInt;

public class MySQLJobBroker implements JobBroker, JobFailedCountManager {
    private final HikariDataSource dataSource;
    private final ObjectMapper mapper;

    public MySQLJobBroker(final String url, // e.g. jdbc:mysql://localhost/euphoriq
                          final String user,
                          final String password,
                          final int connectionNum,
                          final String connectionPoolName) {
        dataSource = new HikariDataSource();
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setJdbcUrl(url);
        dataSource.setUsername(user);
        dataSource.setPassword(password);
        dataSource.setAutoCommit(true);

        dataSource.setMinimumIdle(connectionNum);
        dataSource.setMaximumPoolSize(connectionNum);

        dataSource.setConnectionInitSql(
                "SET SESSION sql_mode = 'TRADITIONAL,NO_AUTO_VALUE_ON_ZERO,ONLY_FULL_GROUP_BY';");

        dataSource.setPoolName(connectionPoolName);
        dataSource.setRegisterMbeans(true);

        dataSource.addDataSourceProperty("characterEncoding", "utf8");
        dataSource.addDataSourceProperty("characterSetResults", "utf8");

        dataSource.addDataSourceProperty("jdbcCompliantTruncation", false);
        dataSource.addDataSourceProperty("alwaysSendSetIsolation", false);
        dataSource.addDataSourceProperty("elideSetAutoCommits", true);
        dataSource.addDataSourceProperty("useServerPrepStmts", false);

        dataSource.addDataSourceProperty("cachePrepStmts", true);
        dataSource.addDataSourceProperty("prepStmtCacheSize", 250);
        dataSource.addDataSourceProperty("prepStmtCacheSqlLimit", 2048);
        dataSource.addDataSourceProperty("cacheServerConfiguration", true);
        dataSource.addDataSourceProperty("useLocalSessionState", true);
        dataSource.addDataSourceProperty("maintainTimeStats", false);

        mapper = new ObjectMapper();
    }

    @Override
    public long enqueue(String queueName, Object arg) {
        return enqueue(queueName, arg, OptionalInt.empty());
    }

    @Override
    public long enqueue(String queueName, Object arg, int timeoutSec) {
        return enqueue(queueName, arg, OptionalInt.of(timeoutSec));
    }

    private long enqueue(String queueName, Object arg, OptionalInt timeoutSecOptional) {
        try (final Connection conn = dataSource.getConnection()) {
            final TinyORM db = new TinyORM(conn);

            final Integer timeoutSec = timeoutSecOptional.isPresent() ? timeoutSecOptional.getAsInt() : null;

            final long id = db.insert(IDPodRow.class).executeSelect().getId();

            db.insert(QueueRow.class)
                    .value("id", id)
                    .value("action_class", arg.getClass())
                    .value("argument", mapper.writeValueAsString(arg))
                    .value("queue_name", queueName)
                    .value("timeout_sec", Optional.ofNullable(timeoutSec))
                    .execute();

            return id;
        } catch (SQLException e) {
            // TODO
            throw new RuntimeException(e);
        } catch (JsonProcessingException e) {
            // TODO
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<Job> dequeue() throws JobCanceledException {
        try (final Connection conn = dataSource.getConnection()) {
            final TinyORM db = new TinyORM(conn);

            final String sql = "SELECT * FROM WHERE queue ORDER BY sequence_number ASC LIMIT 1";

            db.executeQuery("LOCK TABLES queue WRITE");

            final Optional<QueueRow> queueRowOptional =
                    db.singleBySQL(QueueRow.class, sql, Collections.emptyList());
            if (!queueRowOptional.isPresent()) {
                db.executeQuery("UNLOCK TABLES");
                return Optional.empty();
            }

            final QueueRow queueRow = queueRowOptional.get();
            db.delete(queueRow);

            db.executeQuery("UNLOCK TABLES");

            final long id = queueRow.getId();
            final Optional<CanceledJobRow> canceledJobRowOptional =
                    db.single(CanceledJobRow.class).where("id=?", id).execute();
            if (canceledJobRowOptional.isPresent()) {
                // canceled
                db.delete(canceledJobRowOptional.get());
                return Optional.empty();
            }

            final Optional<Integer> timeoutSecOptional = queueRow.getTimeoutSec();
            final OptionalInt timeoutSec = timeoutSecOptional.isPresent() ?
                    OptionalInt.of(timeoutSecOptional.get()) : OptionalInt.empty();
            return Optional.of(new Job(id, queueRow.getArgument(), queueRow.getQueueName(),
                    timeoutSec));
        } catch (SQLException e) {
            // TODO
            throw new RuntimeException(e);
        }
    }

    @Override
    public void cancel(long id) {
        try (final Connection conn = dataSource.getConnection()) {
            final TinyORM db = new TinyORM(conn);
            db.insert(CanceledJobRow.class)
                    .value("id", id)
                    .execute();
        } catch (SQLException e) {
            // TODO
            throw new RuntimeException(e);
        }
    }

    @Override
    public long incrementFailedCount(long id) {
        try (final Connection conn = dataSource.getConnection()) {
            final TinyORM db = new TinyORM(conn);
            return db.insert(FailedJobRow.class)
                    .value("id", id)
                    .value("failed_count", 1)
                    .onDuplicateKeyUpdate("failed_count=failed_count+1")
                    .executeSelect()
                    .getFailedCount();
        } catch (SQLException e) {
            // TODO
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getFailedCount(long id) {
        try (final Connection conn = dataSource.getConnection()) {
            final TinyORM db = new TinyORM(conn);
            final Optional<FailedJobRow> failedJobRowOptional = db.single(FailedJobRow.class)
                    .where("id=?", id)
                    .execute();
            if (!failedJobRowOptional.isPresent()) {
                return 0L;
            }
            return failedJobRowOptional.get().getFailedCount();
        } catch (SQLException e) {
            // TODO
            throw new RuntimeException(e);
        }
    }

    @Table("id_pod")
    @Data
    @EqualsAndHashCode(callSuper = false)
    private static class IDPodRow extends Row<IDPodRow> {
        @PrimaryKey
        @Column("id")
        private long id;
    }

    @Table("queue")
    @Data
    @EqualsAndHashCode(callSuper = false)
    private static class QueueRow extends Row<QueueRow> {
        @PrimaryKey
        @Column("sequence_number")
        private long sequenceNumber;

        @Column("id")
        private long id;

        @Column("action_class")
        private String actionClass;

        @Column("argument")
        private String argument;

        @Column("queue_name")
        private String queueName;

        @Column("timeout_sec")
        private Optional<Integer> timeoutSec;
    }

    @Table("canceled_job")
    @Data
    @EqualsAndHashCode(callSuper = false)
    private static class CanceledJobRow extends Row<CanceledJobRow> {
        @PrimaryKey
        @Column("id")
        private long id;
    }

    @Table("failed_job")
    @Data
    @EqualsAndHashCode(callSuper = false)
    private static class FailedJobRow extends Row<FailedJobRow> {
        @PrimaryKey
        @Column("id")
        private long id;

        @Column("failed_count")
        private long failedCount;
    }
}
