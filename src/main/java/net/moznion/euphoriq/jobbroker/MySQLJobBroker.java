package net.moznion.euphoriq.jobbroker;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import net.moznion.euphoriq.Job;
import net.moznion.euphoriq.exception.JobCanceledException;

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

public class MySQLJobBroker
        implements JobBroker, RetryableJobBroker, JobFailedCountManager, QueueStatusDiscoverer {
    private static final String PROCESSED_JOB_COUNT_TYPE = "processed_job";

    private final HikariDataSource dataSource;
    private final ObjectMapper mapper;

    public MySQLJobBroker(final String url, // e.g. jdbc:mysql://localhost:3306/euphoriq
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
        return enqueue(queueName, arg, arg.getClass().getName(), OptionalInt.empty());
    }

    @Override
    public long enqueue(String queueName, Object arg, int timeoutSec) {
        return enqueue(queueName, arg, arg.getClass().getName(), OptionalInt.of(timeoutSec));
    }

    private long enqueue(String queueName,
                         Object arg,
                         String argumentClass,
                         OptionalInt timeoutSecOptional) {
        try {
            return enqueue(queueName, mapper.writeValueAsString(arg), argumentClass, timeoutSecOptional);
        } catch (JsonProcessingException e) {
            // TODO
            throw new RuntimeException(e);
        }
    }

    private long enqueue(String queueName,
                         String serializedArg,
                         String argumentClass,
                         OptionalInt timeoutSecOptional) {
        try (final Connection conn = dataSource.getConnection()) {
            final TinyORM db = new TinyORM(conn);

            final Integer timeoutSec = timeoutSecOptional.isPresent() ? timeoutSecOptional.getAsInt() : null;

            final long id = db.insert(IDPodRow.class).executeSelect().getId();

            db.insert(QueueRow.class)
              .value("id", id)
              .value("argument_class", argumentClass)
              .value("argument", serializedArg)
              .value("queue_name", queueName)
              .value("timeout_sec", Optional.ofNullable(timeoutSec))
              .execute();

            return id;
        } catch (SQLException e) {
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

            final Optional<Integer> timeoutSecOptional = queueRow.getTimeoutSec();
            final OptionalInt timeoutSec = timeoutSecOptional.isPresent() ?
                                           OptionalInt.of(timeoutSecOptional.get()) : OptionalInt.empty();
            final Job job = new Job(id,
                                    mapper.convertValue(queueRow.getArgument(), Class.forName(
                                            queueRow.getArgumentClass())),
                                    queueRow.getQueueName(),
                                    timeoutSec);

            if (canceledJobRowOptional.isPresent()) {
                // canceled
                db.delete(canceledJobRowOptional.get());
                throw new JobCanceledException(job);
            }

            db.insert(CounterRow.class)
              .value("type", PROCESSED_JOB_COUNT_TYPE)
              .value("count", 1)
              .onDuplicateKeyUpdate("count=count+1")
              .execute();

            return Optional.of(job);
        } catch (SQLException e) {
            // TODO
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
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
            return db.insert(FailedJobCountRow.class)
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
            final Optional<FailedJobCountRow> failedJobRowOptional = db.single(FailedJobCountRow.class)
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

    @Override
    public void retry() {
        try (final Connection conn = dataSource.getConnection()) {
            final TinyORM db = new TinyORM(conn);

            db.executeQuery("LOCK TABLES failed_job WRITE");

            final List<FailedJobRow> failedJobs = db.search(FailedJobRow.class)
                                                    .where("retry_at<=?", Instant.now().getEpochSecond())
                                                    .execute();
            failedJobs.forEach(db::delete);

            db.executeQuery("UNLOCK TABLES");

            for (final FailedJobRow failedJob : failedJobs) {
                final Optional<Integer> timeoutSecOptional = failedJob.getTimeoutSec();
                final OptionalInt timeoutSec = timeoutSecOptional.isPresent() ?
                                               OptionalInt.of(timeoutSecOptional.get()) : OptionalInt.empty();
                enqueue(failedJob.getQueueName(),
                        failedJob.getArgument(),
                        failedJob.getArgumentClass(),
                        timeoutSec);
            }
        } catch (SQLException e) {
            // TODO
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean registerRetryJob(long id, String queueName, Object arg, OptionalInt timeoutSec,
                                    double delay) {
        try (final Connection conn = dataSource.getConnection()) {
            final TinyORM db = new TinyORM(conn);

            final Optional<Integer> timeoutSecOptional =
                    timeoutSec.isPresent() ? Optional.of(timeoutSec.getAsInt()) : Optional.empty();

            db.insert(FailedJobRow.class)
              .value("id", id)
              .value("argument_class", arg.getClass())
              .value("argument", mapper.writeValueAsString(arg))
              .value("queue_name", queueName)
              .value("timeout_sec", timeoutSecOptional)
              .value("retry_at", Instant.now().getEpochSecond() + delay);
        } catch (SQLException e) {
            // TODO
            throw new RuntimeException(e);
        } catch (JsonProcessingException e) {
            // TODO
            throw new RuntimeException(e);
        }
        return true;
    }

    @Override
    public long getNumberOfWaitingJobs() {
        try (final Connection conn = dataSource.getConnection()) {
            final TinyORM db = new TinyORM(conn);
            return db.count(QueueRow.class)
                     .execute();
        } catch (SQLException e) {
            // TODO
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getNumberOfProcessedJobs() {
        try (final Connection conn = dataSource.getConnection()) {
            final TinyORM db = new TinyORM(conn);
            final Optional<CounterRow> processedJobCountRowOptional = db.single(CounterRow.class)
                                                                        .where("type=?",
                                                                               PROCESSED_JOB_COUNT_TYPE)
                                                                        .execute();
            if (!processedJobCountRowOptional.isPresent()) {
                return 0;
            }
            return processedJobCountRowOptional.get().getCount();
        } catch (SQLException e) {
            // TODO
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getNumberOfRetryWaitingJobs() {
        try (final Connection conn = dataSource.getConnection()) {
            final TinyORM db = new TinyORM(conn);
            return db.count(FailedJobRow.class)
                     .execute();
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

        @Column("argument_class")
        private String argumentClass;

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

    // TODO rename
    @Table("failed_job_count")
    @Data
    @EqualsAndHashCode(callSuper = false)
    private static class FailedJobCountRow extends Row<FailedJobCountRow> {
        @PrimaryKey
        @Column("id")
        private long id;

        @Column("failed_count")
        private long failedCount;
    }

    @Table("failed_job")
    @Data
    @EqualsAndHashCode(callSuper = false)
    private static class FailedJobRow extends Row<FailedJobRow> {
        @PrimaryKey
        @Column("id")
        private long id;

        @Column("argument_class")
        private String argumentClass;

        @Column("argument")
        private String argument;

        @Column("queue_name")
        private String queueName;

        @Column("timeout_sec")
        private Optional<Integer> timeoutSec;

        @Column("retry_at")
        private long retryAt;
    }

    @Table("counter")
    @Data
    @EqualsAndHashCode(callSuper = false)
    private static class CounterRow extends Row<CounterRow> {
        @PrimaryKey
        private String type;
        @Column("count")
        private long count;
    }
}
