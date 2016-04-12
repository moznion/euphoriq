CREATE TABLE `id_pod` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`id`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;

CREATE TABLE `queue` (
  `sequence_numper` BIGINT       NOT NULL AUTO_INCREMENT,
  `id`              BIGINT       NOT NULL,
  `action_class`    VARCHAR(255) NOT NULL,
  `argument`        TEXT         NOT NULL,
  `queue_name`      VARCHAR(255) NOT NULL,
  `timeout_sec`     INT                   DEFAULT NULL,
  PRIMARY KEY (`sequence_numper`),
  CONSTRAINT `fk_queue_id` FOREIGN KEY (`id`) REFERENCES `id_pod` (`id`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;

CREATE TABLE `canceled_job` (
  `id` BIGINT NOT NULL,
  PRIMARY KEY (`id`),
  CONSTRAINT `fk_canceled_job_id` FOREIGN KEY (`id`) REFERENCES `id_pod` (`id`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;
