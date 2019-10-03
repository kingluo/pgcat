# pgcat - Enhanced postgresql logical replication

## Why pgcat?

The built-in logicial replication has below shortages:

* only support base table as replication target
* do not filter any origin, which will cause bi-directional dead loop
* could not do table name mapping
* no conflict resolution

pgcat makes below enhancements:

* supports any table type as replication target
e.g. view, fdw, partitioned table, citus distributed table
* only replicates local changes
so that you could make bi-directional replication,
e.g. replicates data between two datacenter
* table name mapping
* optional lww (last-writer-win) conflict resolution
* save replication progress in table, so that it would be logged
when subscriber failovers, it would retain the progress. In contrast,
the built-in logical replication of pg saves the progress in non-logged file.

## Architecture

pgcat is based on logical decoding, and reuses the publication part and pgouput
output plugin of the pg built-in logical replication.

Instead of worker processes and low-level row ingression, pgcat uses sql template
to apply high-level sql commands, so it could make largest compatibility on target
table type. It is written in golang and runs in separate process.

The lww uses one additional jsonb column to store meta info, e.g. timestamp.
It supports timestamp-override columns or cassandra-like counter columns.

## Build from source

pgcat has two parts, pgcat binary and postgresql extension.

### Dependencies

golang >= 1.12

[git-build-rpm](https://github.com/iovation/git-build-rpm)

Assume you uses Centos/RedHat >= 7.0, you needs below rpm packages installed:

```
postgresql11-devel-11.3-1PGDG.rhel7.x86_64
```

### Build pgcat

```
git build-rpm
# check generated rpm, e.g. pgcat-0.1-11568289796.el7.x86_64.rpm
```

### Build pgcat-pgxs

```
export PATH=$PATH:/usr/pgsql-11/bin/
git build-rpm
# check generated rpm, e.g. pgcat-pgxs-0.1-11562916936.el7.x86_64.rpm
```

## Install

### Dependencies

postgresql >= 11.

```
postgresql11-server-11.3-1PGDG.rhel7.x86_64
postgresql11-11.3-1PGDG.rhel7.x86_64
```

### Install pgcat

Install pgcat on individual machine whatever you want.

```
rpm -Uvh pgcat-0.1-11568289796.el7.x86_64.rpm
```

### Install pgcat-pgxs

Install pgcat-pgxs on both the publisher and subscriber databases.

```
rpm -Uvh pgcat-pgxs-0.1-11562916936.el7.x86_64.rpm
```

### Add pgcat user

* Create pgcat user with replication attribute on the publisher database:

```sql
-- adjust your password here
CREATE USER pgcat with REPLICATION PASSWORD 'pgcat';
```

* Create pgcat user on the subscriber database:

```sql
-- adjust your password here
CREATE USER pgcat with PASSWORD 'pgcat';
```

Note: if you do bi-directional replication, create pgcat user with replication
attribute on both publisher and subscriber databases.

### Create extension

Use superuser role to create extension on both the publisher
and subscriber databases.

```sql
CREATE EXTENSION IF NOT EXISTS pgcat;
```

This command would create extension under the pgcat schema.

### Configure the publisher database

* set logical wal level

postgresql.conf

```
wal_level = logical
```

* Allow replication connection to the publisher database

pg_hba.conf

```
host     all     pgcat     0.0.0.0/0     md5
```

Restart pg.

```
pg_ctl restart
```

## Run

### Run pgcat

```bash
mkdir /your/deploy/path
cd /your/deploy/path
cp -a /usr/share/pgcat/pgcat.yml .
# modify pgcat.yml to fit your need
pgcat -c pgcat.yml
```

### Setup the table

#### Grant pgcat

On subscriber database, pgcat needs to read/write the table.

```sql
grant select,insert,update,delete,truncate on foobar to pgcat;
```

If you configure to copy the table in the subscription, then
on publisher database, pgcat needs to select the table.

```sql
grant select on foobar to pgcat;
```

#### Setup publication

On publisher database:

```sql
CREATE PUBLICATION foobar FOR TABLE foobar;
alter publication foobar add table foobar;
```

#### Setup lww (optional)

If you need last-writer-win conflict resolution, then

```
pgcat_setup_lww -c lww.yml
```

Check /usr/share/pgcat/lww.yml for configuration file example.

#### Setup subscription

```sql
INSERT INTO pgcat.pgcat_subscription(name, hostname, port, username, password,
dbname, publications, copy_data, enabled) VALUES ('foobar', '127.0.0.1', 5433,
'pgcat', 'pgcat', 'tmp', '{foobar}', true, true);
```

Then pgcat would start to run your subscription.

## Conflict handling

If not using lww, it's likely to have conflict, e.g. unique violation,
especially for bi-directional replication. When conflict happens, pgcat would
panic this subscription, and restarts it in 1 minute, and so on.

How to handle conflict? As [pg doc](https://www.postgresql.org/docs/current/logical-replication-conflicts.html)
said:

> either by changing data on the subscriber so that
> it does not conflict with the incoming change or by skipping
> the transaction that conflicts with the existing data.

It also works for pgcat. But since pgcat substitutes the subscriber part, so to
skip transaction, you need to use below sql command:

```sql
update pgcat.pgcat_subscription_progress set lsn='0/27FD9B0';
```

The lsn is the lsn of the commit record of this transaction, you could find it
in pgcat log when conflict happens:

```
dml failed, commit_lsn=0/27FD9B0, err=...
```

## Table mapping

You could map publisher table name to different subscriber table name.

You could map multiple tables into one table, gathering multiple data source
into one target, e.g. partition tables, citus shards. Here the target could be
partitioned table, view, or citus main table, so that you could have
heterogeneous layout at different database and do easy replication.

For example, at database1, you have tables `foobar2` and `foobar3`, and you need
to configure them be put into table `foobar2` at database2.

In database2 (subscriber database), run below sql command via superuser:

```sql
insert into pgcat.pgcat_table_mapping(subscription,priority,src,dst)
	values('foobar',1,'^public.foobar[2-3]$','public.foobar2');
```

Note that the regexp and table name should be full qualified,
i.e. with schema prefix. And the regexp is better to be surrounded with
`^` and `$`, otherwise the matching is error prone.

## Replication identity

View and foreign table does not have replica ident, so it needs to configure
them in pgcat. The configuration table is `pgcat.pgcat_replident`.

For example, I need to set `id1` and `id2` columns as replica ident of view
`foobar2_view`.


```sql
insert into pgcat.pgcat_replident values('public.foobar2_view', '{"id1", "id2"}');
```

Note that the table name should be full qualified, i.e. with schema prefix.

## Limitations

* if the target is view, set `copy_data` to `false` in the subscription, and do
not use `instead of` trigger on view, please use view rule instead.
`copy to` view requires `instead of` trigger attached, but the current pg
has no way to set `always` role on the view `instead of` trigger.
On the other hand, pgcat sets its role to `replica`, so `instead of`
trigger would not be called, and then `copy to` view would be no-op.
