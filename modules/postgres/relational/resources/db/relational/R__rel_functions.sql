create or replace function __rel_typed_table_name(
    package_name text,
    module_name text,
    entity_name text,
    kind rel_entity_kind
) returns text as
$$
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
$$ language plpgsql immutable parallel safe strict;

create or replace procedure __rel_initialize_package(package_name text, package_version text, package_id text) as
$$
declare
    pkg bigint;
begin
    select pk from __rel_package pkgs
    where pkgs.name = package_name and pkgs.version = package_version and pkgs.id = package_id
    into pkg;
    if pkg is null then
        insert into __rel_package(name, version, id) values (package_name, package_version, package_id)
        on conflict (name, version, id) do nothing;
    end if;
end;
$$ language plpgsql;

create or replace procedure __rel_initialize_entity(
    package_name text,
    module_name text,
    entity_name text,
    kind rel_entity_kind
) as
$$
declare
    entity bigint;
    tbl    text;
begin
    select pk from __rel_entity e
    where e.package_name = __rel_initialize_entity.package_name
      and e.module_name = __rel_initialize_entity.module_name
      and e.entity_name = __rel_initialize_entity.entity_name
      and e.kind = __rel_initialize_entity.kind
    into entity;
    if entity is null then
        tbl := __rel_typed_table_name(package_name, module_name, entity_name, kind);
        insert into __rel_entity(package_name, module_name, entity_name, kind, base_table)
        values (package_name, module_name, entity_name, kind, tbl);
        if kind = 'template' then
            execute format(
                'create table if not exists %I (
                     contract_pk  bigint primary key references __rel_contracts (contract_pk) on delete cascade,
                     payload_json jsonb not null)',
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
$$ language plpgsql;

create or replace procedure __rel_initialize_choice(
    package_name text,
    module_name text,
    entity_name text,
    kind rel_entity_kind,
    choice text,
    consuming boolean
) as
$$
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
$$ language plpgsql;

create or replace procedure __rel_initialize_implements(
    template_package text,
    template_module text,
    template_entity text,
    interface_package text,
    interface_module text,
    interface_entity text
) as
$$
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
$$ language plpgsql;

create or replace procedure __rel_promote_column(
    package_name text,
    module_name text,
    entity_name text,
    kind rel_entity_kind,
    col text,
    coltype text
) as
$$
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
    if col in ('contract_pk', 'payload_json', 'view_json', 'metadata') then
        raise exception 'projection column % collides with a reserved base column of %', col, tbl;
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
$$ language plpgsql;

create or replace function oldest_checkpoint() returns setof rel_checkpoint as
$$
    select ledger_offset, tx_ix from __rel_transactions order by ledger_offset limit 1;
$$ language sql rows 1 stable parallel safe;

create or replace function latest_checkpoint() returns setof rel_checkpoint as
$$
    select ledger_offset, tx_ix from __rel_watermark where ledger_offset is not null and tx_ix is not null;
$$ language sql rows 1 stable parallel safe;

create or replace function pruned_offset() returns bigint as
$$
    select pruned_offset from __rel_pruning_metadata;
$$ language sql stable parallel safe;

create or replace function rel_validate_offset_exists(p_offset bigint) returns bigint as
$$
declare
    first_offset bigint;
    last_offset  bigint;
begin
    select ledger_offset from oldest_checkpoint() into first_offset;
    select ledger_offset from latest_checkpoint() into last_offset;
    if first_offset is null or p_offset < first_offset then
        raise exception 'offset % is below the retained history', p_offset;
    end if;
    if last_offset is null or p_offset > last_offset then
        raise exception 'offset % is beyond the published watermark', p_offset;
    end if;
    return p_offset;
end;
$$ language plpgsql stable;

create or replace function set_latest(p_offset bigint) returns bigint as
$$
    select set_config('pqs.session_offset_latest', rel_validate_offset_exists(p_offset)::text, false);
    select p_offset;
$$ language sql stable;

create or replace function set_oldest(p_offset bigint) returns bigint as
$$
    select set_config('pqs.session_offset_oldest', rel_validate_offset_exists(p_offset)::text, false);
    select p_offset;
$$ language sql stable;

create or replace function latest_offset() returns bigint as
$$
    select case
               when coalesce(current_setting('pqs.session_offset_latest', true), '') = ''
                   then (select ledger_offset from latest_checkpoint())
               else current_setting('pqs.session_offset_latest', false)::bigint
           end;
$$ language sql stable parallel safe;

create or replace function oldest_offset() returns bigint as
$$
    select case
               when coalesce(current_setting('pqs.session_offset_oldest', true), '') = ''
                   then (select ledger_offset from oldest_checkpoint())
               else current_setting('pqs.session_offset_oldest', false)::bigint
           end;
$$ language sql stable parallel safe;

create or replace function latest_ix() returns bigint as
$$
    select max(tx_ix) from __rel_transactions where ledger_offset <= latest_offset();
$$ language sql stable parallel safe;

create or replace function oldest_ix() returns bigint as
$$
    select min(tx_ix) from __rel_transactions where ledger_offset >= oldest_offset();
$$ language sql stable parallel safe;

create or replace procedure __rel_delete_transactions_after(cutoff_ix bigint) as
$$
declare
    work_exists boolean;
begin
    select exists(select 1 from __rel_transactions where tx_ix > cutoff_ix) into work_exists;
    if work_exists then
        delete from __query_event_visibility
        where event_pk in (select event_pk from __query_events where tx_ix > cutoff_ix);
        delete from __rel_exercises
        where event_pk in (select event_pk from __query_events where tx_ix > cutoff_ix);
        delete from __query_events where tx_ix > cutoff_ix;
        delete from __rel_contract_visibility
        where contract_pk in (select contract_pk from __rel_contracts where created_tx_ix > cutoff_ix);
        update __rel_contracts set archived_tx_ix = null, archived_at_offset = null where archived_tx_ix > cutoff_ix;
        delete from __rel_contracts where created_tx_ix > cutoff_ix;
        delete from __rel_tmp_lifecycle where archived_tx_ix > cutoff_ix;
        delete from __rel_transactions where tx_ix > cutoff_ix;
    end if;
end;
$$ language plpgsql;

create or replace procedure __rel_cleanup_transactions_after_watermark() as
$$
declare
    latest_ix bigint;
begin
    lock table __rel_watermark in exclusive mode;
    select c.tx_ix from latest_checkpoint() c into latest_ix;
    -- a missing checkpoint means a crashed ACS seed; cutoff -1 wipes it including tx_ix=0 (cutoff 0 keeps a real one)
    call __rel_delete_transactions_after(coalesce(latest_ix, -1));
    update __rel_watermark set instance_id = current_setting('scribe.instance');
end;
$$ language plpgsql;
