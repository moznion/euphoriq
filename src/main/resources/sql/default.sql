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
  UNIQUE (`id`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;

CREATE TABLE `canceled_job` (
  `id` BIGINT NOT NULL,
  PRIMARY KEY (`id`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;

CREATE TABLE `failed_job` (
  `id`           BIGINT NOT NULL,
  `failed_count` BIGINT NOT NULL,
  PRIMARY KEY (`id`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;
