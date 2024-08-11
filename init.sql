DO
$do$
BEGIN
   IF NOT EXISTS (
      SELECT
      FROM   pg_catalog.pg_roles
      WHERE  rolname = 'shahar') THEN

      CREATE ROLE sh WITH LOGIN PASSWORD '64478868';
   END IF;
END$do$;

DO
$do$
BEGIN
   IF NOT EXISTS (
      SELECT
      FROM   pg_database
      WHERE  datname = 'cryptodb') THEN

      CREATE DATABASE cryptodb;
      GRANT ALL PRIVILEGES ON DATABASE cryptodb TO shahar;
   END IF;
END
$do$;