RCS Processor
=============

This repository contains several quick hacks aimed at processing RCS flow data in the quickest manner possible. 

It is not recommended for use in any way, shape or form.

```
CREATE TABLE `rcs_flow` (
  `origin_nlc`        CHAR(4) NOT NULL,
  `destination_nlc`   CHAR(4) NOT NULL,
  `route_code`        CHAR(5) NOT NULL,
  `ticket_code`       CHAR(3) NOT NULL,
  `fulfilment_method` SMALLINT UNSIGNED NOT NULL,
  `start_date`        DATE NOT NULL,
  `end_date`          DATE NOT NULL,
  `availability_date` DATE DEFAULT NULL,
  `product_reference` CHAR(4) DEFAULT NULL,
  `season_details`    SET('week','month','3-months','6-months','year') DEFAULT NULL,
  PRIMARY KEY (`origin_nlc`, `destination_nlc`, `route_code`, `ticket_code`, `fulfilment_method`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8;
```

Optimal MySQL config
```
skip-log-bin
innodb_buffer_pool_instances = 16
innodb_buffer_pool_size = 8G
innodb_log_file_size = 4G
max_connections = 500
innodb_file_per_table = ON
innodb_flush_log_at_trx_commit = 2
innodb_flush_method = O_DIRECT
innodb_log_buffer_size = 8M
```