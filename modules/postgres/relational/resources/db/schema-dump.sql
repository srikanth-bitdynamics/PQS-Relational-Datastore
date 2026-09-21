--
-- PostgreSQL database dump
--

\restrict onlyfortestingpqs

-- Dumped from database version 17.10 (Debian 17.10-1.pgdg13+1)
-- Dumped by pg_dump version 17.10 (Debian 17.10-1.pgdg13+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: pqs_relational; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA pqs_relational;


--
-- Name: rel_archive_source; Type: TYPE; Schema: pqs_relational; Owner: -
--

CREATE TYPE pqs_relational.rel_archive_source AS ENUM (
    'native',
    'consuming_exercise'
);


--
-- Name: rel_checkpoint; Type: TYPE; Schema: pqs_relational; Owner: -
--

CREATE TYPE pqs_relational.rel_checkpoint AS (
	ledger_offset bigint,
	tx_ix bigint
);


--
-- Name: rel_entity_kind; Type: TYPE; Schema: pqs_relational; Owner: -
--

CREATE TYPE pqs_relational.rel_entity_kind AS ENUM (
    'template',
    'interface'
);


--
-- Name: rel_event_kind; Type: TYPE; Schema: pqs_relational; Owner: -
--

CREATE TYPE pqs_relational.rel_event_kind AS ENUM (
    'create',
    'exercise',
    'archive',
    'assign',
    'unassign'
);


--
-- Name: rel_index_status; Type: TYPE; Schema: pqs_relational; Owner: -
--

CREATE TYPE pqs_relational.rel_index_status AS ENUM (
    'building',
    'valid',
    'active',
    'retiring',
    'retired'
);


--
-- Name: rel_projection_status; Type: TYPE; Schema: pqs_relational; Owner: -
--

CREATE TYPE pqs_relational.rel_projection_status AS ENUM (
    'draft',
    'active',
    'retired'
);


--
-- Name: rel_source_kind; Type: TYPE; Schema: pqs_relational; Owner: -
--

CREATE TYPE pqs_relational.rel_source_kind AS ENUM (
    'stream',
    'acs_seed',
    'ledger_replay',
    'document_backfill',
    'assignment'
);


--
-- Name: rel_visibility_role; Type: TYPE; Schema: pqs_relational; Owner: -
--

CREATE TYPE pqs_relational.rel_visibility_role AS ENUM (
    'signatory',
    'observer',
    'witness'
);


--
-- Name: __rel_apply_observation_bounds(jsonb); Type: PROCEDURE; Schema: pqs_relational; Owner: -
--

CREATE PROCEDURE pqs_relational.__rel_apply_observation_bounds(IN p_bounds jsonb)
    LANGUAGE plpgsql
    AS $_$
declare
    tbl text;
begin
    update __rel_contracts c
    set created_tx_ix = b.tx_ix, created_at_offset = b.ledger_offset,
        source_kind = b.source::rel_source_kind, history_lower_bound = b.lower_bound,
        creation_synchronizer_id = b.synchronizer
    from jsonb_to_recordset(p_bounds) as b(contract_id text, tx_ix bigint, ledger_offset bigint,
        source text, lower_bound boolean, synchronizer text)
    where c.contract_id = b.contract_id and c.created_tx_ix > b.tx_ix;

    for tbl in
        select distinct e.base_table
        from jsonb_to_recordset(p_bounds) as b(contract_id text)
        join __rel_contracts c on c.contract_id = b.contract_id
        join __rel_entity e on e.pk = c.template_entity_pk
        where e.base_table is not null
    loop
        execute format(
            'update %I p set created_tx_ix = c.created_tx_ix
             from __rel_contracts c
             join jsonb_to_recordset($1) as b(contract_id text) on b.contract_id = c.contract_id
             where p.contract_pk = c.contract_pk and p.created_tx_ix is distinct from c.created_tx_ix',
            tbl
        ) using p_bounds;
    end loop;
end
$_$;


--
-- Name: __rel_begin_maintenance(); Type: PROCEDURE; Schema: pqs_relational; Owner: -
--

CREATE PROCEDURE pqs_relational.__rel_begin_maintenance()
    LANGUAGE plpgsql
    AS $$
begin
    perform pg_advisory_xact_lock(('x70716a5f70726f6a'::bit(64))::bigint);
    if not pg_try_advisory_xact_lock(('x70716a5f7772746c'::bit(64))::bigint) then
        raise exception 'stop the relational writer before pruning or redaction';
    end if;
    perform pg_advisory_xact_lock(('x70716a5f61637476'::bit(64))::bigint);
end;
$$;


--
-- Name: __rel_cleanup_transactions_after_watermark(); Type: PROCEDURE; Schema: pqs_relational; Owner: -
--

CREATE PROCEDURE pqs_relational.__rel_cleanup_transactions_after_watermark()
    LANGUAGE plpgsql
    AS $$
declare
    latest_ix bigint;
begin
    lock table __rel_watermark in exclusive mode;
    select c.tx_ix from latest_checkpoint() c into latest_ix;
    -- a missing checkpoint means a crashed ACS seed; cutoff -1 wipes it including tx_ix=0 (cutoff 0 keeps a real one)
    call __rel_delete_transactions_after(coalesce(latest_ix, -1));
    update __rel_watermark set instance_id = current_setting('scribe.instance');
end;
$$;


--
-- Name: __rel_current_writer(); Type: FUNCTION; Schema: pqs_relational; Owner: -
--

CREATE FUNCTION pqs_relational.__rel_current_writer() RETURNS text
    LANGUAGE sql
    AS $$ select instance_id from __rel_watermark limit 1 $$;


--
-- Name: __rel_delete_transactions_after(bigint); Type: PROCEDURE; Schema: pqs_relational; Owner: -
--

CREATE PROCEDURE pqs_relational.__rel_delete_transactions_after(IN cutoff_ix bigint)
    LANGUAGE plpgsql
    AS $_$
declare
    work_exists boolean;
    tbl         text;
begin
    select exists(select 1 from __rel_transactions where tx_ix > cutoff_ix) into work_exists;
    if work_exists then
        delete from __query_event_visibility
        where event_pk in (select event_pk from __query_events where tx_ix > cutoff_ix);
        delete from __rel_exercises
        where event_pk in (select event_pk from __query_events where tx_ix > cutoff_ix);
        delete from __rel_reassignments
        where event_pk in (select event_pk from __query_events where tx_ix > cutoff_ix);
        delete from __query_events where tx_ix > cutoff_ix;
        delete from __rel_contract_visibility
        where contract_pk in (select contract_pk from __rel_contracts where created_tx_ix > cutoff_ix);
        for tbl in
            select distinct e.base_table
            from __rel_contracts c
            join __rel_entity e on e.pk = c.template_entity_pk
            where c.archived_tx_ix > cutoff_ix and c.created_tx_ix <= cutoff_ix and e.base_table is not null
        loop
            execute format(
                'update %I p set archived_tx_ix = null
                 from __rel_contracts c
                 where p.contract_pk = c.contract_pk and c.archived_tx_ix > $1', tbl
            ) using cutoff_ix;
        end loop;
        update __rel_contracts set archived_tx_ix = null, archived_at_offset = null where archived_tx_ix > cutoff_ix;
        delete from __rel_contracts where created_tx_ix > cutoff_ix;
        delete from __rel_tmp_lifecycle where archived_tx_ix > cutoff_ix;
        delete from __rel_pending_visibility where tx_ix > cutoff_ix;
        delete from __rel_transactions where tx_ix > cutoff_ix;
    end if;
end;
$_$;


--
-- Name: __rel_ensure_writer_valid(); Type: PROCEDURE; Schema: pqs_relational; Owner: -
--

CREATE PROCEDURE pqs_relational.__rel_ensure_writer_valid()
    LANGUAGE plpgsql
    AS $$
declare
    current_writer __rel_watermark.instance_id%type;
    session_writer __rel_watermark.instance_id%type;
begin
    select __rel_current_writer() into current_writer;
    if current_writer is not null then
        session_writer := current_setting('scribe.instance');
        if current_writer != session_writer then
            raise exception 'PQS writer instance has changed (old = %, new = %). Aborting...', session_writer, current_writer;
        end if;
    end if;
end
$$;


--
-- Name: __rel_initialize_choice(text, text, text, pqs_relational.rel_entity_kind, text, boolean); Type: PROCEDURE; Schema: pqs_relational; Owner: -
--

CREATE PROCEDURE pqs_relational.__rel_initialize_choice(IN package_name text, IN module_name text, IN entity_name text, IN kind pqs_relational.rel_entity_kind, IN choice text, IN consuming boolean)
    LANGUAGE plpgsql
    AS $$
declare
    e_pk bigint;
    ch   bigint;
begin
    select pk from __rel_entity e
    where e.package_name = __rel_initialize_choice.package_name
      and e.module_name = __rel_initialize_choice.module_name
      and e.entity_name = __rel_initialize_choice.entity_name
      and e.kind = __rel_initialize_choice.kind
    into e_pk;
    if e_pk is not null then
        select pk from __rel_choice c where c.entity_pk = e_pk and c.choice = __rel_initialize_choice.choice into ch;
        if ch is null then
            insert into __rel_choice(entity_pk, choice, consuming) values (e_pk, choice, consuming);
        end if;
    end if;
end;
$$;


--
-- Name: __rel_initialize_entity(text, text, text, pqs_relational.rel_entity_kind); Type: PROCEDURE; Schema: pqs_relational; Owner: -
--

CREATE PROCEDURE pqs_relational.__rel_initialize_entity(IN package_name text, IN module_name text, IN entity_name text, IN kind pqs_relational.rel_entity_kind)
    LANGUAGE plpgsql
    AS $$
declare
    entity bigint;
    tbl    text;
    qview  text;
begin
    select pk from __rel_entity e
    where e.package_name = __rel_initialize_entity.package_name
      and e.module_name = __rel_initialize_entity.module_name
      and e.entity_name = __rel_initialize_entity.entity_name
      and e.kind = __rel_initialize_entity.kind
    into entity;
    if entity is null then
        tbl := __rel_typed_table_name(package_name, module_name, entity_name, kind);
        qview := case when kind = 'template' then 'q_' || substring(tbl from 5) end;
        insert into __rel_entity(package_name, module_name, entity_name, kind, base_table, query_view)
        values (package_name, module_name, entity_name, kind, tbl, qview);
        if kind = 'template' then
            execute format(
                'create table if not exists %I (
                     contract_pk    bigint primary key references __rel_contracts (contract_pk) on delete cascade,
                     created_tx_ix  bigint not null,
                     archived_tx_ix bigint,
                     payload_json   jsonb not null)',
                tbl
            );
        else
            execute format(
                'create table if not exists %I (
                     contract_pk bigint primary key references __rel_contracts (contract_pk) on delete cascade,
                     view_json   jsonb not null)',
                tbl
            );
        end if;
    end if;
end;
$$;


--
-- Name: __rel_initialize_implements(text, text, text, text, text, text); Type: PROCEDURE; Schema: pqs_relational; Owner: -
--

CREATE PROCEDURE pqs_relational.__rel_initialize_implements(IN template_package text, IN template_module text, IN template_entity text, IN interface_package text, IN interface_module text, IN interface_entity text)
    LANGUAGE plpgsql
    AS $$
declare
    pk_template  bigint;
    pk_interface bigint;
begin
    select pk from __rel_entity e
    where e.package_name = template_package and e.module_name = template_module
      and e.entity_name = template_entity and e.kind = 'template'
    into pk_template;
    select pk from __rel_entity e
    where e.package_name = interface_package and e.module_name = interface_module
      and e.entity_name = interface_entity and e.kind = 'interface'
    into pk_interface;
    if pk_template is not null and pk_interface is not null
        and not exists (select 1 from __rel_implements i
                        where i.template_pk = pk_template and i.interface_pk = pk_interface) then
        insert into __rel_implements(template_pk, interface_pk) values (pk_template, pk_interface);
    end if;
end;
$$;


--
-- Name: __rel_initialize_package(text, text, text); Type: PROCEDURE; Schema: pqs_relational; Owner: -
--

CREATE PROCEDURE pqs_relational.__rel_initialize_package(IN package_name text, IN package_version text, IN package_id text)
    LANGUAGE plpgsql
    AS $$
declare
    existing_name    text;
    existing_version text;
begin
    insert into __rel_package(name, version, id) values (package_name, package_version, package_id)
    on conflict (id) do nothing;
    select name, version from __rel_package pkgs where pkgs.id = package_id
    into existing_name, existing_version;
    if existing_name is distinct from package_name or existing_version is distinct from package_version then
        raise exception 'package id % is registered as %:% but was reinitialised as %:%',
            package_id, existing_name, existing_version, package_name, package_version;
    end if;
end;
$$;


--
-- Name: __rel_pin_encoding(boolean, boolean, boolean); Type: PROCEDURE; Schema: pqs_relational; Owner: -
--

CREATE PROCEDURE pqs_relational.__rel_pin_encoding(IN p_numeric boolean, IN p_int64 boolean, IN p_exclude boolean)
    LANGUAGE plpgsql
    AS $$
declare
    existing __rel_encoding%rowtype;
begin
    insert into __rel_encoding (singleton, numeric_as_string, int64_as_string, exclude_nulls)
    values (true, p_numeric, p_int64, p_exclude)
    on conflict (singleton) do nothing;
    select * from __rel_encoding into existing;
    if existing.numeric_as_string is distinct from p_numeric
       or existing.int64_as_string is distinct from p_int64
       or existing.exclude_nulls is distinct from p_exclude then
        raise exception 'datastore payload encoding is pinned as (%, %, %) but the writer is configured as (%, %, %)',
            existing.numeric_as_string, existing.int64_as_string, existing.exclude_nulls, p_numeric, p_int64, p_exclude;
    end if;
end;
$$;


--
-- Name: __rel_promote_column(text, text, text, pqs_relational.rel_entity_kind, text, text); Type: PROCEDURE; Schema: pqs_relational; Owner: -
--

CREATE PROCEDURE pqs_relational.__rel_promote_column(IN package_name text, IN module_name text, IN entity_name text, IN kind pqs_relational.rel_entity_kind, IN col text, IN coltype text)
    LANGUAGE plpgsql
    AS $$
declare
    tbl      text;
    existing text;
begin
    if octet_length(col) > 63 then
        raise exception 'projection column % is % bytes, over the 63-byte identifier limit', col, octet_length(col);
    end if;
    select base_table from __rel_entity e
    where e.package_name = __rel_promote_column.package_name
      and e.module_name = __rel_promote_column.module_name
      and e.entity_name = __rel_promote_column.entity_name
      and e.kind = __rel_promote_column.kind
    into tbl;
    if tbl is null then
        raise exception 'relational entity %:%:% (%) is not initialized; run schema apply first',
            package_name, module_name, entity_name, kind;
    end if;
    if col in ('contract_pk', 'payload_json', 'view_json', 'metadata',
               'contract_id', 'representative_package_id', 'creation_package_id',
               'created_tx_ix', 'archived_tx_ix', 'created_at_offset', 'creation_synchronizer_id',
               'signatories', 'observers') then
        raise exception 'projection column % collides with a reserved base or query-view column of %', col, tbl;
    end if;
    select case data_type
               when 'numeric' then format('numeric(%s, %s)', numeric_precision, numeric_scale)
               when 'timestamp with time zone' then 'timestamptz'
               else data_type
           end
    from information_schema.columns c
    where c.table_schema = current_schema() and c.table_name = tbl and c.column_name = col
    into existing;
    if existing is null then
        execute format('alter table %I add column %I %s', tbl, col, coltype);
    elsif existing <> coltype then
        raise exception 'projection column %.% is % but the projection expects %', tbl, col, existing, coltype;
    end if;
end
$$;


--
-- Name: __rel_prune(bigint, boolean); Type: FUNCTION; Schema: pqs_relational; Owner: -
--

CREATE FUNCTION pqs_relational.__rel_prune(p_offset bigint, p_dry_run boolean) RETURNS TABLE(pruning_boundary_offset bigint, deleted_contracts bigint, deleted_exercises bigint, deleted_events bigint, deleted_transactions bigint)
    LANGUAGE plpgsql
    AS $$
declare
    cutoff bigint;
    watermark rel_checkpoint;
begin
    call __rel_begin_maintenance();
    select * into watermark from latest_checkpoint();
    if p_offset is null or p_offset < 0 or watermark.ledger_offset is null or p_offset > watermark.ledger_offset then
        raise exception 'pruning offset must be within published history';
    end if;
    if p_offset <= coalesce(pruned_offset(), -1) then
        return query select null::bigint, 0::bigint, 0::bigint, 0::bigint, 0::bigint;
        return;
    end if;
    select max(tx_ix) into cutoff from __rel_transactions where ledger_offset <= p_offset;
    if cutoff is null then
        return query select null::bigint, 0::bigint, 0::bigint, 0::bigint, 0::bigint;
        return;
    end if;

    with removed_contracts as (
        select contract_pk from __rel_contracts where archived_tx_ix <= cutoff and created_tx_ix <= cutoff
    ), kept_contracts as (
        select * from __rel_contracts where contract_pk not in (select contract_pk from removed_contracts)
    ), removed_events as (
        select e.event_pk from __query_events e where e.tx_ix <= cutoff
          and not exists (select 1 from kept_contracts c where c.contract_id = e.contract_id
                          and (e.event_kind = 'create' or c.contract_pk = e.event_pk))
    )
    select (select count(*) from removed_contracts),
           (select count(*) from __rel_exercises where event_pk in (select event_pk from removed_events)),
           (select count(*) from removed_events),
           (select count(*) from __rel_transactions t where t.tx_ix < cutoff and t.tx_ix <> watermark.tx_ix
              and not exists (select 1 from kept_contracts c where c.created_tx_ix = t.tx_ix or c.archived_tx_ix = t.tx_ix)
              and not exists (select 1 from __query_events e where e.tx_ix = t.tx_ix
                              and e.event_pk not in (select event_pk from removed_events)))
    into deleted_contracts, deleted_exercises, deleted_events, deleted_transactions;

    if not p_dry_run then
        insert into __rel_contract_tombstone(contract_id)
        select contract_id from __rel_contracts where archived_tx_ix <= cutoff and created_tx_ix <= cutoff
        union select contract_id from __rel_tmp_lifecycle where archived_tx_ix <= cutoff
        on conflict do nothing;
        delete from __rel_contract_visibility where contract_pk in (
            select contract_pk from __rel_contracts where archived_tx_ix <= cutoff and created_tx_ix <= cutoff);
        delete from __rel_contracts where archived_tx_ix <= cutoff and created_tx_ix <= cutoff;
        delete from __query_event_visibility where event_pk in (
            select e.event_pk from __query_events e where e.tx_ix <= cutoff
            and not exists (select 1 from __rel_contracts c where c.contract_id = e.contract_id
                            and (e.event_kind = 'create' or c.contract_pk = e.event_pk)));
        delete from __rel_exercises where event_pk in (
            select e.event_pk from __query_events e where e.tx_ix <= cutoff
            and not exists (select 1 from __rel_contracts c where c.contract_id = e.contract_id
                            and (e.event_kind = 'create' or c.contract_pk = e.event_pk)));
        delete from __rel_reassignments where event_pk in (
            select e.event_pk from __query_events e where e.tx_ix <= cutoff
            and not exists (select 1 from __rel_contracts c where c.contract_id = e.contract_id
                            and (e.event_kind = 'create' or c.contract_pk = e.event_pk)));
        delete from __query_events e where e.tx_ix <= cutoff
            and not exists (select 1 from __rel_contracts c where c.contract_id = e.contract_id
                            and (e.event_kind = 'create' or c.contract_pk = e.event_pk));
        delete from __rel_tmp_lifecycle where archived_tx_ix <= cutoff;
        delete from __rel_transactions t where t.tx_ix < cutoff and t.tx_ix <> watermark.tx_ix
            and not exists (select 1 from __rel_contracts c where c.created_tx_ix = t.tx_ix or c.archived_tx_ix = t.tx_ix)
            and not exists (select 1 from __query_events e where e.tx_ix = t.tx_ix);
        update __rel_pruning_metadata set pruned_offset = p_offset;
        update __query_coverage set create_history_complete = false, exercise_history_complete = false,
            archive_history_complete = false, archive_visibility_complete = false,
            reassignment_history_complete = false, assignment_origin_state_complete = false
        where actual_from_offset <= p_offset;
    end if;
    return query select p_offset, deleted_contracts, deleted_exercises, deleted_events, deleted_transactions;
end;
$$;


--
-- Name: __rel_typed_table_name(text, text, text, pqs_relational.rel_entity_kind); Type: FUNCTION; Schema: pqs_relational; Owner: -
--

CREATE FUNCTION pqs_relational.__rel_typed_table_name(package_name text, module_name text, entity_name text, kind pqs_relational.rel_entity_kind) RETURNS text
    LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
    AS $$
declare
    prefix    text := case when kind = 'interface' then 'relv_' else 'rel_' end;
    raw       text := package_name || ':' || module_name || ':' || entity_name;
    lp        text := lower(package_name);
    lm        text := lower(module_name);
    le        text := lower(entity_name);
    sp        text := regexp_replace(lp, '[^a-z0-9]', '_', 'g');
    sm        text := regexp_replace(lm, '[^a-z0-9]', '_', 'g');
    se        text := regexp_replace(le, '[^a-z0-9]', '_', 'g');
    slug      text := sp || '__' || sm || '__' || se;
    candidate text := prefix || slug;
    lossless  boolean := sp = lp and sm = lm and se = le
                     and lp = package_name and lm = module_name and le = entity_name
                     and position('__' in lp) = 0
                     and position('__' in lm) = 0
                     and position('__' in le) = 0;
    budget    int := 63 - length(prefix) - 14;
begin
    if lossless and length(candidate) <= 63 then
        return candidate;
    end if;
    return prefix || left(slug, budget) || '_h' || left(md5(raw), 12);
end;
$$;


--
-- Name: __rel_update_watermark_fn(); Type: FUNCTION; Schema: pqs_relational; Owner: -
--

CREATE FUNCTION pqs_relational.__rel_update_watermark_fn() RETURNS trigger
    LANGUAGE plpgsql
    AS $_$
declare
    tbl text;
begin
    -- Bypass the writer check when instance_id is being updated explicitly, so a writer reset can invalidate its predecessor.
    if new.instance_id = old.instance_id then
        call __rel_ensure_writer_valid();
    end if;

    if new.tx_ix is null or new.ledger_offset is null then
        raise exception '__rel_watermark.tx_ix and __rel_watermark.ledger_offset must not be null';
    end if;

    -- Keep archives whose create/assignment has not arrived yet; streams across synchronizers can be non-causal.
    update __rel_contracts c
    set archived_tx_ix = d.archived_tx_ix, archived_at_offset = d.archived_at_offset
    from (
        select distinct on (contract_id) contract_id, archived_tx_ix, archived_at_offset
        from __rel_tmp_lifecycle where archived_tx_ix <= new.tx_ix
        order by contract_id, archived_tx_ix
    ) d
    where c.contract_id = d.contract_id and c.created_tx_ix <= new.tx_ix
      and (c.archived_tx_ix is null or d.archived_tx_ix < c.archived_tx_ix);

    for tbl in
        select distinct e.base_table
        from __rel_tmp_lifecycle d
        join __rel_contracts c on c.contract_id = d.contract_id
        join __rel_entity e on e.pk = c.template_entity_pk
        where d.archived_tx_ix <= new.tx_ix and c.created_tx_ix <= new.tx_ix and e.base_table is not null
    loop
        execute format(
            'update %I p set archived_tx_ix = c.archived_tx_ix
             from __rel_contracts c
             join __rel_tmp_lifecycle d on d.contract_id = c.contract_id
             where p.contract_pk = c.contract_pk and d.archived_tx_ix <= $1 and c.created_tx_ix <= $1
               and p.archived_tx_ix is distinct from c.archived_tx_ix',
            tbl
        ) using new.tx_ix;
    end loop;

    delete from __rel_tmp_lifecycle d
    using __rel_contracts c
    where c.contract_id = d.contract_id and c.created_tx_ix <= new.tx_ix and d.archived_tx_ix <= new.tx_ix;

    insert into __rel_contract_visibility(contract_pk, party, role)
    select c.contract_pk, v.party, v.role from __rel_pending_visibility v
    join __rel_contracts c using (contract_id)
    where v.tx_ix <= new.tx_ix and c.created_tx_ix <= new.tx_ix and c.redaction_id is null
    on conflict do nothing;
    delete from __rel_pending_visibility where tx_ix <= new.tx_ix;

    update __query_coverage
    set through_offset = new.ledger_offset
    where instance_id = new.instance_id and completed_at is null;

    return new;
end;
$_$;


--
-- Name: latest_checkpoint(); Type: FUNCTION; Schema: pqs_relational; Owner: -
--

CREATE FUNCTION pqs_relational.latest_checkpoint() RETURNS SETOF pqs_relational.rel_checkpoint
    LANGUAGE sql STABLE ROWS 1 PARALLEL SAFE
    AS $$
    select ledger_offset, tx_ix from __rel_watermark where ledger_offset is not null and tx_ix is not null;
$$;


--
-- Name: latest_ix(); Type: FUNCTION; Schema: pqs_relational; Owner: -
--

CREATE FUNCTION pqs_relational.latest_ix() RETURNS bigint
    LANGUAGE sql STABLE PARALLEL SAFE
    AS $$
    select max(tx_ix) from __rel_transactions where ledger_offset <= latest_offset();
$$;


--
-- Name: latest_offset(); Type: FUNCTION; Schema: pqs_relational; Owner: -
--

CREATE FUNCTION pqs_relational.latest_offset() RETURNS bigint
    LANGUAGE sql STABLE PARALLEL SAFE
    AS $$
    select rel_validate_offset_exists(least(nullif(current_setting('pqs.session_offset_latest', true), '')::bigint, ledger_offset))
    from latest_checkpoint();
$$;


--
-- Name: oldest_checkpoint(); Type: FUNCTION; Schema: pqs_relational; Owner: -
--

CREATE FUNCTION pqs_relational.oldest_checkpoint() RETURNS SETOF pqs_relational.rel_checkpoint
    LANGUAGE sql STABLE ROWS 1 PARALLEL SAFE
    AS $$
    select ledger_offset, tx_ix from __rel_transactions order by ledger_offset limit 1;
$$;


--
-- Name: oldest_ix(); Type: FUNCTION; Schema: pqs_relational; Owner: -
--

CREATE FUNCTION pqs_relational.oldest_ix() RETURNS bigint
    LANGUAGE sql STABLE PARALLEL SAFE
    AS $$
    select min(tx_ix) from __rel_transactions where ledger_offset >= oldest_offset();
$$;


--
-- Name: oldest_offset(); Type: FUNCTION; Schema: pqs_relational; Owner: -
--

CREATE FUNCTION pqs_relational.oldest_offset() RETURNS bigint
    LANGUAGE sql STABLE PARALLEL SAFE
    AS $$
    select case
               when coalesce(current_setting('pqs.session_offset_oldest', true), '') = ''
                   then (select greatest(ledger_offset, pruned_offset()) from oldest_checkpoint())
               else rel_validate_offset_exists(current_setting('pqs.session_offset_oldest', false)::bigint)
           end;
$$;


--
-- Name: prune_archived_to_offset(bigint); Type: FUNCTION; Schema: pqs_relational; Owner: -
--

CREATE FUNCTION pqs_relational.prune_archived_to_offset(p_offset bigint) RETURNS TABLE(pruning_boundary_offset bigint, deleted_contracts bigint, deleted_exercises bigint, deleted_events bigint, deleted_transactions bigint)
    LANGUAGE sql
    AS $$ select * from __rel_prune(p_offset, false); $$;


--
-- Name: prune_archived_to_offset_dry_run(bigint); Type: FUNCTION; Schema: pqs_relational; Owner: -
--

CREATE FUNCTION pqs_relational.prune_archived_to_offset_dry_run(p_offset bigint) RETURNS TABLE(pruning_boundary_offset bigint, deleted_contracts bigint, deleted_exercises bigint, deleted_events bigint, deleted_transactions bigint)
    LANGUAGE sql
    AS $$ select * from __rel_prune(p_offset, true); $$;


--
-- Name: pruned_offset(); Type: FUNCTION; Schema: pqs_relational; Owner: -
--

CREATE FUNCTION pqs_relational.pruned_offset() RETURNS bigint
    LANGUAGE sql STABLE PARALLEL SAFE
    AS $$
    select pruned_offset from __rel_pruning_metadata;
$$;


--
-- Name: redact_contract(text, text); Type: FUNCTION; Schema: pqs_relational; Owner: -
--

CREATE FUNCTION pqs_relational.redact_contract(p_contract_id text, p_redaction_id text) RETURNS bigint
    LANGUAGE plpgsql
    AS $_$
declare
    c __rel_contracts%rowtype;
    entity record;
    boundary bigint;
begin
    call __rel_begin_maintenance();
    if p_redaction_id is null or btrim(p_redaction_id) = '' then
        raise exception 'redaction id must not be empty';
    end if;
    select * into c from __rel_contracts where contract_id = p_contract_id for update;
    if not found then
        if exists (select 1 from __rel_redaction where contract_id = p_contract_id and redaction_id = p_redaction_id) then
            return 0;
        end if;
        raise exception 'cannot find contract %', p_contract_id;
    end if;
    if c.redaction_id = p_redaction_id then return 0; end if;
    if c.redaction_id is not null then raise exception 'contract % is already redacted', p_contract_id; end if;
    select tx_ix into boundary from latest_checkpoint();
    if boundary is null or c.created_tx_ix > boundary or c.archived_tx_ix is null or c.archived_tx_ix > boundary then
        raise exception 'contract % must be archived through the published watermark', p_contract_id;
    end if;

    -- Removing the entire payload row also removes every promoted value and its index entries.
    for entity in select base_table from __rel_entity
                  where pk = c.template_entity_pk or pk in (
                      select interface_pk from __rel_implements where template_pk = c.template_entity_pk)
    loop
        execute format('delete from %I where contract_pk = $1', entity.base_table) using c.contract_pk;
    end loop;
    update __rel_contracts set contract_key_json = null, contract_key_hash = null,
        metadata = null, redaction_id = p_redaction_id where contract_pk = c.contract_pk;
    update __rel_exercises x set argument_json = null, result_json = null, redaction_id = p_redaction_id
    from __query_events e where e.event_pk = x.event_pk and e.contract_id = p_contract_id
      and x.redaction_id is null;
    insert into __rel_redaction(contract_id, redaction_id, redacted_at)
    values (p_contract_id, p_redaction_id, now());
    return 1;
end;
$_$;


--
-- Name: redact_exercise(bigint, integer, text); Type: FUNCTION; Schema: pqs_relational; Owner: -
--

CREATE FUNCTION pqs_relational.redact_exercise(p_offset bigint, p_node integer, p_redaction_id text) RETURNS bigint
    LANGUAGE plpgsql
    AS $$
declare
    event_id bigint;
    previous text;
begin
    call __rel_begin_maintenance();
    if p_redaction_id is null or btrim(p_redaction_id) = '' then
        raise exception 'redaction id must not be empty';
    end if;
    select x.event_pk, x.redaction_id into event_id, previous
    from __rel_exercises x join __query_events e using (event_pk)
    where e.ledger_offset = p_offset and e.node_id = p_node
      and e.tx_ix <= (select tx_ix from latest_checkpoint()) for update of x;
    if not found then
        if exists (select 1 from __rel_redaction where event_id_offset = p_offset
                   and event_id_node = p_node and redaction_id = p_redaction_id) then return 0; end if;
        raise exception 'cannot find published exercise (%, %)', p_offset, p_node;
    end if;
    if previous = p_redaction_id then return 0; end if;
    if previous is not null then raise exception 'exercise (%, %) is already redacted', p_offset, p_node; end if;
    update __rel_exercises set argument_json = null, result_json = null, redaction_id = p_redaction_id
    where event_pk = event_id;
    insert into __rel_redaction(event_id_offset, event_id_node, redaction_id, redacted_at)
    values (p_offset, p_node, p_redaction_id, now());
    return 1;
end;
$$;


--
-- Name: rel_validate_offset_exists(bigint); Type: FUNCTION; Schema: pqs_relational; Owner: -
--

CREATE FUNCTION pqs_relational.rel_validate_offset_exists(p_offset bigint) RETURNS bigint
    LANGUAGE plpgsql STABLE
    AS $$
declare
    first_offset bigint;
    last_offset  bigint;
begin
    select greatest(ledger_offset, pruned_offset()) from oldest_checkpoint() into first_offset;
    select ledger_offset from latest_checkpoint() into last_offset;
    if first_offset is null or p_offset < first_offset then
        raise exception 'offset % is below the retained history', p_offset;
    end if;
    if last_offset is null or p_offset > last_offset then
        raise exception 'offset % is beyond the published watermark', p_offset;
    end if;
    return p_offset;
end;
$$;


--
-- Name: set_latest(bigint); Type: FUNCTION; Schema: pqs_relational; Owner: -
--

CREATE FUNCTION pqs_relational.set_latest(p_offset bigint) RETURNS bigint
    LANGUAGE sql STABLE
    AS $$
    select set_config('pqs.session_offset_latest', rel_validate_offset_exists(p_offset)::text, false);
    select p_offset;
$$;


--
-- Name: set_oldest(bigint); Type: FUNCTION; Schema: pqs_relational; Owner: -
--

CREATE FUNCTION pqs_relational.set_oldest(p_offset bigint) RETURNS bigint
    LANGUAGE sql STABLE
    AS $$
    select set_config('pqs.session_offset_oldest', rel_validate_offset_exists(p_offset)::text, false);
    select p_offset;
$$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: __query_coverage; Type: TABLE; Schema: pqs_relational; Owner: -
--

CREATE TABLE pqs_relational.__query_coverage (
    coverage_id bigint NOT NULL,
    instance_id text,
    source_kind pqs_relational.rel_source_kind NOT NULL,
    requested_from_offset bigint,
    actual_from_offset bigint,
    through_offset bigint,
    source_pruned_offset bigint,
    acs_seed_offset bigint,
    ingested_all_parties boolean NOT NULL,
    ingested_parties text[],
    contract_filter text,
    metadata_filter text,
    tree_stream boolean NOT NULL,
    create_history_complete boolean NOT NULL,
    exercise_history_complete boolean NOT NULL,
    archive_history_complete boolean NOT NULL,
    archive_visibility_complete boolean NOT NULL,
    reassignment_history_complete boolean NOT NULL,
    assignment_origin_state_complete boolean NOT NULL,
    started_at timestamp with time zone NOT NULL,
    completed_at timestamp with time zone
);


--
-- Name: __query_coverage_coverage_id_seq; Type: SEQUENCE; Schema: pqs_relational; Owner: -
--

CREATE SEQUENCE pqs_relational.__query_coverage_coverage_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: __query_coverage_coverage_id_seq; Type: SEQUENCE OWNED BY; Schema: pqs_relational; Owner: -
--

ALTER SEQUENCE pqs_relational.__query_coverage_coverage_id_seq OWNED BY pqs_relational.__query_coverage.coverage_id;


--
-- Name: __query_event_visibility; Type: TABLE; Schema: pqs_relational; Owner: -
--

CREATE TABLE pqs_relational.__query_event_visibility (
    event_pk bigint NOT NULL,
    party text NOT NULL
);


--
-- Name: __query_events; Type: TABLE; Schema: pqs_relational; Owner: -
--

CREATE TABLE pqs_relational.__query_events (
    event_pk bigint NOT NULL,
    tx_ix bigint NOT NULL,
    ledger_offset bigint NOT NULL,
    node_id integer NOT NULL,
    contract_id text,
    template_entity_pk bigint NOT NULL,
    event_kind pqs_relational.rel_event_kind NOT NULL,
    source_kind pqs_relational.rel_source_kind NOT NULL,
    archive_source pqs_relational.rel_archive_source,
    visibility_complete boolean NOT NULL
);


--
-- Name: __query_projection; Type: TABLE; Schema: pqs_relational; Owner: -
--

CREATE TABLE pqs_relational.__query_projection (
    projection_version bigint NOT NULL,
    definition jsonb NOT NULL,
    definition_hash text NOT NULL,
    shape_hash text NOT NULL,
    resolved_shape jsonb NOT NULL,
    layout smallint NOT NULL,
    status pqs_relational.rel_projection_status NOT NULL,
    backfilled_through_ix bigint,
    created_at timestamp with time zone NOT NULL,
    activated_at timestamp with time zone
);


--
-- Name: __rel_backfill_progress; Type: TABLE; Schema: pqs_relational; Owner: -
--

CREATE TABLE pqs_relational.__rel_backfill_progress (
    shape_hash text NOT NULL,
    qualified text NOT NULL,
    cursor_tx_ix bigint DEFAULT '-1'::integer NOT NULL,
    cursor_pk bigint DEFAULT 0 NOT NULL,
    through_ix bigint NOT NULL,
    completed boolean DEFAULT false NOT NULL
);


--
-- Name: __rel_choice; Type: TABLE; Schema: pqs_relational; Owner: -
--

CREATE TABLE pqs_relational.__rel_choice (
    pk bigint NOT NULL,
    entity_pk bigint NOT NULL,
    choice text NOT NULL,
    consuming boolean NOT NULL
);


--
-- Name: __rel_choice_pk_seq; Type: SEQUENCE; Schema: pqs_relational; Owner: -
--

CREATE SEQUENCE pqs_relational.__rel_choice_pk_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: __rel_choice_pk_seq; Type: SEQUENCE OWNED BY; Schema: pqs_relational; Owner: -
--

ALTER SEQUENCE pqs_relational.__rel_choice_pk_seq OWNED BY pqs_relational.__rel_choice.pk;


--
-- Name: __rel_contract_tombstone; Type: TABLE; Schema: pqs_relational; Owner: -
--

CREATE TABLE pqs_relational.__rel_contract_tombstone (
    contract_id text NOT NULL
);


--
-- Name: __rel_contract_visibility; Type: TABLE; Schema: pqs_relational; Owner: -
--

CREATE TABLE pqs_relational.__rel_contract_visibility (
    contract_pk bigint NOT NULL,
    party text NOT NULL,
    role pqs_relational.rel_visibility_role NOT NULL
);


--
-- Name: __rel_contracts; Type: TABLE; Schema: pqs_relational; Owner: -
--

CREATE TABLE pqs_relational.__rel_contracts (
    contract_pk bigint NOT NULL,
    contract_id text NOT NULL,
    template_entity_pk bigint NOT NULL,
    representative_package_id text NOT NULL,
    creation_package_id text,
    created_tx_ix bigint NOT NULL,
    archived_tx_ix bigint,
    created_at_offset bigint,
    archived_at_offset bigint,
    life_ix int8range GENERATED ALWAYS AS (
CASE
    WHEN (archived_tx_ix < created_tx_ix) THEN 'empty'::int8range
    ELSE int8range(created_tx_ix, archived_tx_ix, '[)'::text)
END) STORED,
    signatories text[] DEFAULT '{}'::text[] NOT NULL,
    observers text[] DEFAULT '{}'::text[] NOT NULL,
    create_witnesses text[] DEFAULT '{}'::text[] NOT NULL,
    creation_synchronizer_id text,
    metadata bytea,
    contract_key_json jsonb,
    contract_key_hash bytea,
    divulged_only boolean DEFAULT false NOT NULL,
    redaction_id text,
    source_kind pqs_relational.rel_source_kind NOT NULL,
    history_lower_bound boolean DEFAULT false NOT NULL
);


--
-- Name: __rel_encoding; Type: TABLE; Schema: pqs_relational; Owner: -
--

CREATE TABLE pqs_relational.__rel_encoding (
    singleton boolean DEFAULT true NOT NULL,
    numeric_as_string boolean NOT NULL,
    int64_as_string boolean NOT NULL,
    exclude_nulls boolean NOT NULL,
    CONSTRAINT __rel_encoding_singleton_check CHECK (singleton)
);


--
-- Name: __rel_entity; Type: TABLE; Schema: pqs_relational; Owner: -
--

CREATE TABLE pqs_relational.__rel_entity (
    pk bigint NOT NULL,
    package_name text NOT NULL,
    module_name text NOT NULL,
    entity_name text NOT NULL,
    kind pqs_relational.rel_entity_kind NOT NULL,
    base_table text,
    query_view text
);


--
-- Name: __rel_entity_pk_seq; Type: SEQUENCE; Schema: pqs_relational; Owner: -
--

CREATE SEQUENCE pqs_relational.__rel_entity_pk_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: __rel_entity_pk_seq; Type: SEQUENCE OWNED BY; Schema: pqs_relational; Owner: -
--

ALTER SEQUENCE pqs_relational.__rel_entity_pk_seq OWNED BY pqs_relational.__rel_entity.pk;


--
-- Name: __rel_exercises; Type: TABLE; Schema: pqs_relational; Owner: -
--

CREATE TABLE pqs_relational.__rel_exercises (
    event_pk bigint NOT NULL,
    choice_entity_pk bigint,
    contract_template_entity_pk bigint,
    choice_name text NOT NULL,
    consuming boolean NOT NULL,
    controllers text[] DEFAULT '{}'::text[] NOT NULL,
    argument_json jsonb,
    result_json jsonb,
    last_descendant_node_id integer,
    redaction_id text
);


--
-- Name: __rel_implements; Type: TABLE; Schema: pqs_relational; Owner: -
--

CREATE TABLE pqs_relational.__rel_implements (
    template_pk bigint NOT NULL,
    interface_pk bigint NOT NULL
);


--
-- Name: __rel_managed_index; Type: TABLE; Schema: pqs_relational; Owner: -
--

CREATE TABLE pqs_relational.__rel_managed_index (
    index_id bigint NOT NULL,
    projection_version bigint,
    table_name text NOT NULL,
    index_name text NOT NULL,
    definition text NOT NULL,
    columns text[] NOT NULL,
    opclasses text[],
    status pqs_relational.rel_index_status NOT NULL,
    adopted boolean DEFAULT false NOT NULL,
    covered_query_shapes text[],
    created_at timestamp with time zone NOT NULL,
    validated_at timestamp with time zone,
    physical_oid oid
);


--
-- Name: __rel_managed_index_index_id_seq; Type: SEQUENCE; Schema: pqs_relational; Owner: -
--

CREATE SEQUENCE pqs_relational.__rel_managed_index_index_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: __rel_managed_index_index_id_seq; Type: SEQUENCE OWNED BY; Schema: pqs_relational; Owner: -
--

ALTER SEQUENCE pqs_relational.__rel_managed_index_index_id_seq OWNED BY pqs_relational.__rel_managed_index.index_id;


--
-- Name: __rel_package; Type: TABLE; Schema: pqs_relational; Owner: -
--

CREATE TABLE pqs_relational.__rel_package (
    pk bigint NOT NULL,
    name text NOT NULL,
    version text NOT NULL,
    id text NOT NULL
);


--
-- Name: __rel_package_pk_seq; Type: SEQUENCE; Schema: pqs_relational; Owner: -
--

CREATE SEQUENCE pqs_relational.__rel_package_pk_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: __rel_package_pk_seq; Type: SEQUENCE OWNED BY; Schema: pqs_relational; Owner: -
--

ALTER SEQUENCE pqs_relational.__rel_package_pk_seq OWNED BY pqs_relational.__rel_package.pk;


--
-- Name: __rel_pending_visibility; Type: TABLE; Schema: pqs_relational; Owner: -
--

CREATE TABLE pqs_relational.__rel_pending_visibility (
    contract_id text NOT NULL,
    party text NOT NULL,
    role pqs_relational.rel_visibility_role NOT NULL,
    tx_ix bigint NOT NULL
);


--
-- Name: __rel_pruning_metadata; Type: TABLE; Schema: pqs_relational; Owner: -
--

CREATE TABLE pqs_relational.__rel_pruning_metadata (
    singleton boolean DEFAULT true NOT NULL,
    pruned_offset bigint,
    CONSTRAINT __rel_pruning_metadata_singleton_check CHECK (singleton)
);


--
-- Name: __rel_reassignments; Type: TABLE; Schema: pqs_relational; Owner: -
--

CREATE TABLE pqs_relational.__rel_reassignments (
    event_pk bigint NOT NULL,
    reassignment_id text NOT NULL,
    source_synchronizer_id text NOT NULL,
    target_synchronizer_id text NOT NULL,
    submitter text,
    reassignment_counter bigint NOT NULL,
    assignment_exclusivity timestamp with time zone
);


--
-- Name: __rel_redaction; Type: TABLE; Schema: pqs_relational; Owner: -
--

CREATE TABLE pqs_relational.__rel_redaction (
    id bigint NOT NULL,
    contract_id text,
    event_id_offset bigint,
    event_id_node integer,
    redaction_id text NOT NULL,
    redacted_at timestamp with time zone NOT NULL
);


--
-- Name: __rel_redaction_id_seq; Type: SEQUENCE; Schema: pqs_relational; Owner: -
--

CREATE SEQUENCE pqs_relational.__rel_redaction_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: __rel_redaction_id_seq; Type: SEQUENCE OWNED BY; Schema: pqs_relational; Owner: -
--

ALTER SEQUENCE pqs_relational.__rel_redaction_id_seq OWNED BY pqs_relational.__rel_redaction.id;


--
-- Name: __rel_tmp_lifecycle; Type: TABLE; Schema: pqs_relational; Owner: -
--

CREATE TABLE pqs_relational.__rel_tmp_lifecycle (
    contract_id text NOT NULL,
    archived_tx_ix bigint NOT NULL,
    archived_at_offset bigint
);


--
-- Name: __rel_transactions; Type: TABLE; Schema: pqs_relational; Owner: -
--

CREATE TABLE pqs_relational.__rel_transactions (
    tx_ix bigint NOT NULL,
    ledger_offset bigint NOT NULL,
    transaction_id text,
    effective_at timestamp with time zone,
    synchronizer_id text,
    workflow_id text,
    external_transaction_hash bytea,
    paid_traffic_cost bigint
);


--
-- Name: __rel_watermark; Type: TABLE; Schema: pqs_relational; Owner: -
--

CREATE TABLE pqs_relational.__rel_watermark (
    singleton boolean DEFAULT true NOT NULL,
    tx_ix bigint,
    ledger_offset bigint,
    instance_id text,
    CONSTRAINT __rel_watermark_singleton_check CHECK (singleton)
);


--
-- Name: active_contracts; Type: VIEW; Schema: pqs_relational; Owner: -
--

CREATE VIEW pqs_relational.active_contracts AS
 SELECT contract_pk,
    contract_id,
    template_entity_pk,
    representative_package_id,
    created_tx_ix,
    created_at_offset,
    signatories,
    observers,
    creation_synchronizer_id
   FROM pqs_relational.__rel_contracts c
  WHERE ((life_ix @> ( SELECT pqs_relational.latest_ix() AS latest_ix)) AND (NOT divulged_only) AND (redaction_id IS NULL));


--
-- Name: flyway_schema_history; Type: TABLE; Schema: pqs_relational; Owner: -
--

CREATE TABLE pqs_relational.flyway_schema_history (
    installed_rank integer NOT NULL,
    version character varying(50),
    description character varying(200) NOT NULL,
    type character varying(20) NOT NULL,
    script character varying(1000) NOT NULL,
    checksum integer,
    installed_by character varying(100) NOT NULL,
    installed_on timestamp without time zone DEFAULT now() NOT NULL,
    execution_time integer NOT NULL,
    success boolean NOT NULL
);


--
-- Name: reassignments; Type: VIEW; Schema: pqs_relational; Owner: -
--

CREATE VIEW pqs_relational.reassignments AS
 SELECT e.ledger_offset,
    e.node_id,
    e.tx_ix,
    e.contract_id,
    e.event_kind,
    r.reassignment_id,
    r.source_synchronizer_id,
    r.target_synchronizer_id,
    r.submitter,
    r.reassignment_counter,
    r.assignment_exclusivity
   FROM (pqs_relational.__query_events e
     JOIN pqs_relational.__rel_reassignments r USING (event_pk))
  WHERE ((e.ledger_offset >= ( SELECT pqs_relational.oldest_offset() AS oldest_offset)) AND (e.ledger_offset <= ( SELECT pqs_relational.latest_offset() AS latest_offset)));


--
-- Name: rel_com_digitalasset_pqs_schema_postgres_relation_h4e2efdb40345; Type: TABLE; Schema: pqs_relational; Owner: -
--

CREATE TABLE pqs_relational.rel_com_digitalasset_pqs_schema_postgres_relation_h4e2efdb40345 (
    contract_pk bigint NOT NULL,
    created_tx_ix bigint NOT NULL,
    archived_tx_ix bigint,
    payload_json jsonb NOT NULL
);


--
-- Name: transactions; Type: VIEW; Schema: pqs_relational; Owner: -
--

CREATE VIEW pqs_relational.transactions AS
 SELECT tx_ix,
    ledger_offset,
    transaction_id,
    effective_at,
    synchronizer_id,
    workflow_id,
    external_transaction_hash,
    paid_traffic_cost
   FROM pqs_relational.__rel_transactions t
  WHERE ((ledger_offset >= ( SELECT pqs_relational.oldest_offset() AS oldest_offset)) AND (ledger_offset <= ( SELECT pqs_relational.latest_offset() AS latest_offset)));


--
-- Name: __query_coverage coverage_id; Type: DEFAULT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__query_coverage ALTER COLUMN coverage_id SET DEFAULT nextval('pqs_relational.__query_coverage_coverage_id_seq'::regclass);


--
-- Name: __rel_choice pk; Type: DEFAULT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__rel_choice ALTER COLUMN pk SET DEFAULT nextval('pqs_relational.__rel_choice_pk_seq'::regclass);


--
-- Name: __rel_entity pk; Type: DEFAULT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__rel_entity ALTER COLUMN pk SET DEFAULT nextval('pqs_relational.__rel_entity_pk_seq'::regclass);


--
-- Name: __rel_managed_index index_id; Type: DEFAULT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__rel_managed_index ALTER COLUMN index_id SET DEFAULT nextval('pqs_relational.__rel_managed_index_index_id_seq'::regclass);


--
-- Name: __rel_package pk; Type: DEFAULT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__rel_package ALTER COLUMN pk SET DEFAULT nextval('pqs_relational.__rel_package_pk_seq'::regclass);


--
-- Name: __rel_redaction id; Type: DEFAULT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__rel_redaction ALTER COLUMN id SET DEFAULT nextval('pqs_relational.__rel_redaction_id_seq'::regclass);


--
-- Name: __query_coverage __query_coverage_pkey; Type: CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__query_coverage
    ADD CONSTRAINT __query_coverage_pkey PRIMARY KEY (coverage_id);


--
-- Name: __query_event_visibility __query_event_visibility_pkey; Type: CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__query_event_visibility
    ADD CONSTRAINT __query_event_visibility_pkey PRIMARY KEY (event_pk, party);


--
-- Name: __query_events __query_events_ledger_offset_node_id_key; Type: CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__query_events
    ADD CONSTRAINT __query_events_ledger_offset_node_id_key UNIQUE (ledger_offset, node_id);


--
-- Name: __query_events __query_events_pkey; Type: CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__query_events
    ADD CONSTRAINT __query_events_pkey PRIMARY KEY (event_pk);


--
-- Name: __query_projection __query_projection_pkey; Type: CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__query_projection
    ADD CONSTRAINT __query_projection_pkey PRIMARY KEY (projection_version);


--
-- Name: __rel_backfill_progress __rel_backfill_progress_pkey; Type: CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__rel_backfill_progress
    ADD CONSTRAINT __rel_backfill_progress_pkey PRIMARY KEY (shape_hash, qualified);


--
-- Name: __rel_choice __rel_choice_entity_pk_choice_key; Type: CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__rel_choice
    ADD CONSTRAINT __rel_choice_entity_pk_choice_key UNIQUE (entity_pk, choice);


--
-- Name: __rel_choice __rel_choice_pkey; Type: CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__rel_choice
    ADD CONSTRAINT __rel_choice_pkey PRIMARY KEY (pk);


--
-- Name: __rel_contract_tombstone __rel_contract_tombstone_pkey; Type: CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__rel_contract_tombstone
    ADD CONSTRAINT __rel_contract_tombstone_pkey PRIMARY KEY (contract_id);


--
-- Name: __rel_contract_visibility __rel_contract_visibility_pkey; Type: CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__rel_contract_visibility
    ADD CONSTRAINT __rel_contract_visibility_pkey PRIMARY KEY (contract_pk, party, role);


--
-- Name: __rel_contracts __rel_contracts_contract_id_key; Type: CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__rel_contracts
    ADD CONSTRAINT __rel_contracts_contract_id_key UNIQUE (contract_id);


--
-- Name: __rel_contracts __rel_contracts_pkey; Type: CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__rel_contracts
    ADD CONSTRAINT __rel_contracts_pkey PRIMARY KEY (contract_pk);


--
-- Name: __rel_encoding __rel_encoding_pkey; Type: CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__rel_encoding
    ADD CONSTRAINT __rel_encoding_pkey PRIMARY KEY (singleton);


--
-- Name: __rel_entity __rel_entity_base_table_key; Type: CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__rel_entity
    ADD CONSTRAINT __rel_entity_base_table_key UNIQUE (base_table);


--
-- Name: __rel_entity __rel_entity_package_name_module_name_entity_name_kind_key; Type: CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__rel_entity
    ADD CONSTRAINT __rel_entity_package_name_module_name_entity_name_kind_key UNIQUE (package_name, module_name, entity_name, kind);


--
-- Name: __rel_entity __rel_entity_pkey; Type: CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__rel_entity
    ADD CONSTRAINT __rel_entity_pkey PRIMARY KEY (pk);


--
-- Name: __rel_entity __rel_entity_query_view_key; Type: CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__rel_entity
    ADD CONSTRAINT __rel_entity_query_view_key UNIQUE (query_view);


--
-- Name: __rel_exercises __rel_exercises_pkey; Type: CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__rel_exercises
    ADD CONSTRAINT __rel_exercises_pkey PRIMARY KEY (event_pk);


--
-- Name: __rel_implements __rel_implements_pkey; Type: CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__rel_implements
    ADD CONSTRAINT __rel_implements_pkey PRIMARY KEY (template_pk, interface_pk);


--
-- Name: __rel_managed_index __rel_managed_index_pkey; Type: CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__rel_managed_index
    ADD CONSTRAINT __rel_managed_index_pkey PRIMARY KEY (index_id);


--
-- Name: __rel_package __rel_package_id_key; Type: CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__rel_package
    ADD CONSTRAINT __rel_package_id_key UNIQUE (id);


--
-- Name: __rel_package __rel_package_pkey; Type: CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__rel_package
    ADD CONSTRAINT __rel_package_pkey PRIMARY KEY (pk);


--
-- Name: __rel_pending_visibility __rel_pending_visibility_pkey; Type: CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__rel_pending_visibility
    ADD CONSTRAINT __rel_pending_visibility_pkey PRIMARY KEY (tx_ix, contract_id, party, role);


--
-- Name: __rel_pruning_metadata __rel_pruning_metadata_pkey; Type: CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__rel_pruning_metadata
    ADD CONSTRAINT __rel_pruning_metadata_pkey PRIMARY KEY (singleton);


--
-- Name: __rel_reassignments __rel_reassignments_pkey; Type: CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__rel_reassignments
    ADD CONSTRAINT __rel_reassignments_pkey PRIMARY KEY (event_pk);


--
-- Name: __rel_redaction __rel_redaction_pkey; Type: CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__rel_redaction
    ADD CONSTRAINT __rel_redaction_pkey PRIMARY KEY (id);


--
-- Name: __rel_transactions __rel_transactions_ledger_offset_tx_ix_key; Type: CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__rel_transactions
    ADD CONSTRAINT __rel_transactions_ledger_offset_tx_ix_key UNIQUE (ledger_offset, tx_ix);


--
-- Name: __rel_transactions __rel_transactions_pkey; Type: CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__rel_transactions
    ADD CONSTRAINT __rel_transactions_pkey PRIMARY KEY (tx_ix);


--
-- Name: __rel_watermark __rel_watermark_pkey; Type: CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__rel_watermark
    ADD CONSTRAINT __rel_watermark_pkey PRIMARY KEY (singleton);


--
-- Name: flyway_schema_history flyway_schema_history_pk; Type: CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.flyway_schema_history
    ADD CONSTRAINT flyway_schema_history_pk PRIMARY KEY (installed_rank);


--
-- Name: rel_com_digitalasset_pqs_schema_postgres_relation_h4e2efdb40345 rel_com_digitalasset_pqs_schema_postgres_relation_h4e2efdb_pkey; Type: CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.rel_com_digitalasset_pqs_schema_postgres_relation_h4e2efdb40345
    ADD CONSTRAINT rel_com_digitalasset_pqs_schema_postgres_relation_h4e2efdb_pkey PRIMARY KEY (contract_pk);


--
-- Name: __query_event_visibility_party_idx; Type: INDEX; Schema: pqs_relational; Owner: -
--

CREATE INDEX __query_event_visibility_party_idx ON pqs_relational.__query_event_visibility USING btree (party, event_pk);


--
-- Name: __query_events_contract_id_idx; Type: INDEX; Schema: pqs_relational; Owner: -
--

CREATE INDEX __query_events_contract_id_idx ON pqs_relational.__query_events USING hash (contract_id);


--
-- Name: __query_events_tx_ix_idx; Type: INDEX; Schema: pqs_relational; Owner: -
--

CREATE INDEX __query_events_tx_ix_idx ON pqs_relational.__query_events USING btree (tx_ix);


--
-- Name: __query_projection_active_idx; Type: INDEX; Schema: pqs_relational; Owner: -
--

CREATE UNIQUE INDEX __query_projection_active_idx ON pqs_relational.__query_projection USING btree ((true)) WHERE (status = 'active'::pqs_relational.rel_projection_status);


--
-- Name: __query_projection_live_hash_idx; Type: INDEX; Schema: pqs_relational; Owner: -
--

CREATE UNIQUE INDEX __query_projection_live_hash_idx ON pqs_relational.__query_projection USING btree (definition_hash) WHERE (status <> 'retired'::pqs_relational.rel_projection_status);


--
-- Name: __rel_contract_visibility_party_idx; Type: INDEX; Schema: pqs_relational; Owner: -
--

CREATE INDEX __rel_contract_visibility_party_idx ON pqs_relational.__rel_contract_visibility USING btree (party, contract_pk);


--
-- Name: __rel_contracts_current_idx; Type: INDEX; Schema: pqs_relational; Owner: -
--

CREATE INDEX __rel_contracts_current_idx ON pqs_relational.__rel_contracts USING btree (template_entity_pk, created_tx_ix DESC, contract_pk DESC) WHERE ((archived_tx_ix IS NULL) AND (divulged_only = false) AND (redaction_id IS NULL));


--
-- Name: __rel_contracts_life_idx; Type: INDEX; Schema: pqs_relational; Owner: -
--

CREATE INDEX __rel_contracts_life_idx ON pqs_relational.__rel_contracts USING gist (life_ix) WHERE (NOT divulged_only);


--
-- Name: __rel_contracts_template_created_idx; Type: INDEX; Schema: pqs_relational; Owner: -
--

CREATE INDEX __rel_contracts_template_created_idx ON pqs_relational.__rel_contracts USING btree (template_entity_pk, created_tx_ix DESC, contract_pk DESC);


--
-- Name: __rel_redaction_contract_idx; Type: INDEX; Schema: pqs_relational; Owner: -
--

CREATE INDEX __rel_redaction_contract_idx ON pqs_relational.__rel_redaction USING btree (contract_id);


--
-- Name: __rel_redaction_event_idx; Type: INDEX; Schema: pqs_relational; Owner: -
--

CREATE INDEX __rel_redaction_event_idx ON pqs_relational.__rel_redaction USING btree (event_id_offset, event_id_node);


--
-- Name: __rel_tmp_lifecycle_contract_idx; Type: INDEX; Schema: pqs_relational; Owner: -
--

CREATE INDEX __rel_tmp_lifecycle_contract_idx ON pqs_relational.__rel_tmp_lifecycle USING btree (contract_id);


--
-- Name: __rel_tmp_lifecycle_ix_idx; Type: INDEX; Schema: pqs_relational; Owner: -
--

CREATE INDEX __rel_tmp_lifecycle_ix_idx ON pqs_relational.__rel_tmp_lifecycle USING btree (archived_tx_ix);


--
-- Name: __rel_transactions_offset_idx; Type: INDEX; Schema: pqs_relational; Owner: -
--

CREATE INDEX __rel_transactions_offset_idx ON pqs_relational.__rel_transactions USING btree (ledger_offset);


--
-- Name: flyway_schema_history_s_idx; Type: INDEX; Schema: pqs_relational; Owner: -
--

CREATE INDEX flyway_schema_history_s_idx ON pqs_relational.flyway_schema_history USING btree (success);


--
-- Name: __rel_watermark __rel_update_watermark_trg; Type: TRIGGER; Schema: pqs_relational; Owner: -
--

CREATE TRIGGER __rel_update_watermark_trg BEFORE UPDATE OF tx_ix ON pqs_relational.__rel_watermark FOR EACH ROW EXECUTE FUNCTION pqs_relational.__rel_update_watermark_fn();


--
-- Name: __query_events __query_events_template_entity_pk_fkey; Type: FK CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__query_events
    ADD CONSTRAINT __query_events_template_entity_pk_fkey FOREIGN KEY (template_entity_pk) REFERENCES pqs_relational.__rel_entity(pk);


--
-- Name: __rel_choice __rel_choice_entity_pk_fkey; Type: FK CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__rel_choice
    ADD CONSTRAINT __rel_choice_entity_pk_fkey FOREIGN KEY (entity_pk) REFERENCES pqs_relational.__rel_entity(pk);


--
-- Name: __rel_contracts __rel_contracts_template_entity_pk_fkey; Type: FK CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__rel_contracts
    ADD CONSTRAINT __rel_contracts_template_entity_pk_fkey FOREIGN KEY (template_entity_pk) REFERENCES pqs_relational.__rel_entity(pk);


--
-- Name: __rel_exercises __rel_exercises_choice_entity_pk_fkey; Type: FK CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__rel_exercises
    ADD CONSTRAINT __rel_exercises_choice_entity_pk_fkey FOREIGN KEY (choice_entity_pk) REFERENCES pqs_relational.__rel_choice(pk);


--
-- Name: __rel_exercises __rel_exercises_contract_template_entity_pk_fkey; Type: FK CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__rel_exercises
    ADD CONSTRAINT __rel_exercises_contract_template_entity_pk_fkey FOREIGN KEY (contract_template_entity_pk) REFERENCES pqs_relational.__rel_entity(pk);


--
-- Name: __rel_implements __rel_implements_interface_pk_fkey; Type: FK CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__rel_implements
    ADD CONSTRAINT __rel_implements_interface_pk_fkey FOREIGN KEY (interface_pk) REFERENCES pqs_relational.__rel_entity(pk);


--
-- Name: __rel_implements __rel_implements_template_pk_fkey; Type: FK CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__rel_implements
    ADD CONSTRAINT __rel_implements_template_pk_fkey FOREIGN KEY (template_pk) REFERENCES pqs_relational.__rel_entity(pk);


--
-- Name: __rel_managed_index __rel_managed_index_projection_version_fkey; Type: FK CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.__rel_managed_index
    ADD CONSTRAINT __rel_managed_index_projection_version_fkey FOREIGN KEY (projection_version) REFERENCES pqs_relational.__query_projection(projection_version);


--
-- Name: rel_com_digitalasset_pqs_schema_postgres_relation_h4e2efdb40345 rel_com_digitalasset_pqs_schema_postgres_relat_contract_pk_fkey; Type: FK CONSTRAINT; Schema: pqs_relational; Owner: -
--

ALTER TABLE ONLY pqs_relational.rel_com_digitalasset_pqs_schema_postgres_relation_h4e2efdb40345
    ADD CONSTRAINT rel_com_digitalasset_pqs_schema_postgres_relat_contract_pk_fkey FOREIGN KEY (contract_pk) REFERENCES pqs_relational.__rel_contracts(contract_pk) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

\unrestrict onlyfortestingpqs

