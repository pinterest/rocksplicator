-- !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
-- ALWAYS BACKUP YOUR DATA BEFORE EXECUTING THIS SCRIPT
-- !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

# This script creates db tables for rocksplicator controller.

DROP TABLE IF EXISTS task;
DROP TABLE IF EXISTS tag;

CREATE TABLE IF NOT EXISTS tag (
  namespace VARCHAR(128) NOT NULL,
  name VARCHAR(128) NOT NULL,
  locks TINYINT UNSIGNED NOT NULL,
  created_at DATETIME NOT NULL,
  owner VARCHAR(256),
  PRIMARY KEY (namespace, name)
) ENGINE=INNODB;

CREATE TABLE IF NOT EXISTS task (
  id BIGINT AUTO_INCREMENT,
  name VARCHAR(128),
  priority TINYINT UNSIGNED NOT NULL, # 0 is the highest priority
  state TINYINT UNSIGNED NOT NULL, # 0: Pending, 1: Running, 2: Done, 3: FAILED
  tag_namespace VARCHAR(128) NOT NULL,
  tag_name VARCHAR(128) NOT NULL,
  body TEXT NOT NULL,
  created_at DATETIME NOT NULL,
  run_after DATETIME NOT NULL,
  claimed_worker VARCHAR(128),
  last_alive_at DATETIME,
  output TEXT,
  PRIMARY KEY (id),
  FOREIGN KEY (tag_namespace, tag_name) REFERENCES tag(namespace, name) ON UPDATE RESTRICT ON DELETE CASCADE
) ENGINE=INNODB;
