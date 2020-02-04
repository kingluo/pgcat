# pgcat - Enhanced postgresql logical replication

* [Why pgcat?](#why-pgcat)
* [Architecture](#architecture)
* [Build from source](#build-from-source)
* [Install](#install)
* [Run](#run)
* [Conflict handling](#conflict-handling)
* [Table mapping](#table-mapping)
* [Replication identity](#replication-identity)
* [Limitations](#limitations)
* [Conflict resolution](#conflict-resolution)

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
git clone https://github.com/kingluo/pgcat-pgxs
cd pgcat-pgxs
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

If you need last-writer-win conflict resolution, then run `pgcat_setup_lww` on all pg instances.

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

The pgcat would check `pgcat_subscription`, if it changes, pgcat would apply the changes.

### Run pgcat

```bash
mkdir /your/deploy/path
cd /your/deploy/path
cp -a /usr/share/pgcat/pgcat.yml .
# modify pgcat.yml to fit your need
pgcat -c pgcat.yml
```

pgcat uses golang proxy dialer, so if you need to access your database via proxy, you could run below command:

```bash
all_proxy=socks5h://127.0.0.1:20000 pgcat -c pgcat.yml
```

If you need to run pgcat in daemon on Linux, just use `setsid` command:

```bash
setsid pgcat -c pgcat.yml &>/dev/null
```

**Admin HTTP API**:

If you configure `admin_listen_address`, e.g. `admin_listen_address: 127.0.0.1:30000`,
then you could run below commands to admin the pgcat process:

```bash
# rotate the log file
# you could use logrotate to rotate pgcat log files as you need
curl http://127.0.0.1:30000/rotate

# reload the yaml config file, e.g. you could add new databases to replicate
curl http://127.0.0.1:30000/reload
```

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

## Conflict resolution

Conflict resolution is necessary for logical replication.

Logical replication is normally used for loose-coupling different pg deployments
(or different pg HA deployments, one HA deployment consists of one master and
multiple slaves, where they are connected via physical replication),
especially for different data centers, where does no require
real-time data consistence).

For example, we have two groups of pg:

pg1 consists of pg1-master and pg1-slave, run in datacenter1.

pg2 consists of pg2-master, pg2-slave1 and pg2-slave2, run in datacenter2.

They replicate data changes to each other, but not in real time, and the
network between data centers may be broken for time to time.

Each data center changes data independently, which would normally involves data
with same identity, e.g. same primary key. Such data needs some sort of policy
to keep consistent in both data centers.

For example, pg1 writes "foo" to row foo, before this change is applied to pg2,
pg2 writes "bar" to row foo. Then which would be the final version of value?

### lww (Last-writer-win)

Similar to Cassandra, it could use write timestamp to resolve the conflict.
The write with latest timestamp would be the last version of data value. The
pg deployment would keep time in sync, e.g. via NTP.

The `pgcat_setup_lww` command is used to setup the table for used by lww. Note
that conflict resolution is optional, so if you're sure the data changes are
consistent by nature or by design, you do not need to run this command on your
tables.

How does it work?

It would category the columns as:
* Inline columns
Inline columns have the same timestamp as the row.
* Individual columns
Individual columns have their own timestamp associated.
* Counter columns
Each pg deployment would have their own individual copy of counter value. But
when you read it, it would sum up all copys as the final value.

When you run `pgcat_setup_lww`, it would:

* Add a new column `__pgcat_lww` to the target table, which
is in `jsonb` type, used to record the meta info of the row and columns.
* for existing rows, it would populate `__pgcat_lww` concurrently (i.e. would
not block concurrent data r/w when the table setup is processing).
* Create a trigger, which would
	* for local change, get the current timestamp as the row timestamp
	* for remote change, get the row timestamp in `__pgcat_lww`
	* for inline columns, compare the row timestamp
	* for each individual column, compare its own timestamp
	* for counter column, store the remote change in `__pgcat_lww`, use
	system identifier from `pg_control_system()` to identify different
	remote peers
* Create a view to filter the `__pgcat_lww` so that it would not be exported
to application level. For example, if you have table `foobar`,
then it would create a view `foobar_pgcat_lww_view`.
* Create a helper function `pgcat_lww_counter()` used to sum up counter column.
You could call `pgcat_lww_counter(__pgcat_lww, 'foobar')` to
get the value of column `foobar`.
* When you delete a row, it would not be really deleted, instead, it would be
marked as tombstone in `__pgcat_lww`, so `pgcat_setup_lww` would create index
for tombstone rows and create a helper function to vacuum them whenever you need,
e.g. remove tombstones older than 3 days:
`foobar_pgcat_lww_vaccum(interval '3 days') `.
