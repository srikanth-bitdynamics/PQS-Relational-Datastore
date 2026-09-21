-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create type rel_event_kind as enum ('create', 'exercise', 'archive');
create type rel_source_kind as enum ('stream', 'acs_seed', 'ledger_replay', 'document_backfill');
create type rel_archive_source as enum ('native', 'consuming_exercise');
create type rel_visibility_role as enum ('signatory', 'observer', 'witness');
create type rel_projection_status as enum ('draft', 'active', 'retired');
create type rel_index_status as enum ('building', 'valid', 'active', 'retiring', 'retired');
create type rel_entity_kind as enum ('template', 'interface');

create type rel_checkpoint as (ledger_offset bigint, tx_ix bigint);

create table __rel_watermark (
    singleton     boolean primary key default true check (singleton),
    tx_ix         bigint,
    ledger_offset bigint,
    instance_id   text
);

create table __rel_pruning_metadata (
    singleton     boolean primary key default true check (singleton),
    pruned_offset bigint
);

create table __rel_transactions (
    tx_ix                     bigint primary key,
    ledger_offset             bigint not null,
    transaction_id            text,
    effective_at              timestamptz,
    synchronizer_id           text,
    workflow_id               text,
    external_transaction_hash bytea,
    paid_traffic_cost         bigint,
    unique (ledger_offset, tx_ix)
);

create table __rel_package (
    pk      bigserial primary key,
    name    text not null,
    version text not null,
    id      text not null
);

create table __rel_entity (
    pk           bigserial primary key,
    package_name text not null,
    module_name  text not null,
    entity_name  text not null,
    kind         rel_entity_kind not null,
    unique (package_name, module_name, entity_name, kind)
);

create table __rel_choice (
    pk        bigserial primary key,
    entity_pk bigint not null references __rel_entity (pk),
    choice    text not null,
    consuming boolean not null,
    unique (entity_pk, choice)
);

create table __rel_implements (
    template_pk  bigint not null,
    interface_pk bigint not null,
    primary key (template_pk, interface_pk)
);

create table __rel_contracts (
    contract_pk               bigint primary key,
    contract_id               text not null unique,
    template_entity_pk        bigint not null references __rel_entity (pk),
    representative_package_id text not null,
    creation_package_id       text,
    created_tx_ix             bigint not null,
    archived_tx_ix            bigint,
    created_at_offset         bigint,
    archived_at_offset        bigint,
    life_ix                   int8range generated always as (int8range(created_tx_ix, archived_tx_ix, '[)')) stored,
    signatories               text[] not null default '{}',
    observers                 text[] not null default '{}',
    create_witnesses          text[] not null default '{}',
    creation_synchronizer_id  text,
    metadata                  bytea,
    contract_key_json         jsonb,
    contract_key_hash         bytea,
    divulged_only             boolean not null default false,
    redaction_id              text,
    source_kind               rel_source_kind not null,
    history_lower_bound       boolean not null default false
);

create table __query_events (
    event_pk            bigint primary key,
    tx_ix               bigint not null,
    ledger_offset       bigint not null,
    node_id             integer not null,
    contract_id         text,
    template_entity_pk  bigint not null references __rel_entity (pk),
    event_kind          rel_event_kind not null,
    source_kind         rel_source_kind not null,
    archive_source      rel_archive_source,
    visibility_complete boolean not null,
    unique (ledger_offset, node_id)
);

create table __query_event_visibility (
    event_pk bigint not null,
    party    text not null,
    primary key (event_pk, party)
);

create table __rel_contract_visibility (
    contract_pk bigint not null,
    party       text not null,
    role        rel_visibility_role not null,
    primary key (contract_pk, party, role)
);

create table __rel_exercises (
    event_pk                    bigint primary key,
    choice_entity_pk            bigint references __rel_choice (pk),
    contract_template_entity_pk bigint references __rel_entity (pk),
    choice_name                 text not null,
    consuming                   boolean not null,
    controllers                 text[] not null default '{}',
    argument_json               jsonb,
    result_json                 jsonb,
    last_descendant_node_id     integer,
    redaction_id                text
);

create table __query_coverage (
    coverage_id                      bigserial primary key,
    source_kind                      rel_source_kind not null,
    requested_from_offset            bigint,
    actual_from_offset               bigint,
    through_offset                   bigint,
    source_pruned_offset             bigint,
    acs_seed_offset                  bigint,
    ingested_all_parties             boolean not null,
    ingested_parties                 text[],
    contract_filter                  text,
    metadata_filter                  text,
    tree_stream                      boolean not null,
    create_history_complete          boolean not null,
    exercise_history_complete        boolean not null,
    archive_history_complete         boolean not null,
    archive_visibility_complete      boolean not null,
    reassignment_history_complete    boolean not null,
    assignment_origin_state_complete boolean not null,
    started_at                       timestamptz not null,
    completed_at                     timestamptz
);

create table __query_projection (
    projection_version    bigint primary key,
    definition            jsonb not null,
    definition_hash       text not null,
    encoding_flags        jsonb not null,
    layout                smallint not null,
    status                rel_projection_status not null,
    backfilled_through_ix bigint,
    created_at            timestamptz not null,
    activated_at          timestamptz
);

create table __rel_managed_index (
    index_id             bigserial primary key,
    projection_version   bigint,
    table_name           text not null,
    index_name           text not null,
    definition           text not null,
    columns              text[] not null,
    opclasses            text[],
    status               rel_index_status not null,
    adopted              boolean not null default false,
    covered_query_shapes text[],
    created_at           timestamptz not null,
    validated_at         timestamptz
);

create table __rel_redaction (
    id             bigserial primary key,
    contract_id    text,
    event_id_offset bigint,
    event_id_node  integer,
    redaction_id   text not null,
    redacted_at    timestamptz not null
);

create table __rel_tmp_lifecycle (
    contract_id        text not null,
    archived_tx_ix     bigint not null,
    archived_at_offset bigint
);

create index __rel_transactions_offset_idx on __rel_transactions (ledger_offset);
create index __rel_contracts_life_idx on __rel_contracts using gist (life_ix) where not divulged_only;
create index __rel_contracts_template_created_idx
    on __rel_contracts (template_entity_pk, created_tx_ix desc, contract_pk desc);
create index __rel_contracts_current_idx
    on __rel_contracts (template_entity_pk, created_tx_ix desc, contract_pk desc)
    where archived_tx_ix is null and divulged_only = false and redaction_id is null;
create index __query_events_tx_ix_idx on __query_events (tx_ix);
create index __query_events_contract_id_idx on __query_events using hash (contract_id);
create index __query_event_visibility_party_idx on __query_event_visibility (party, event_pk);
create index __rel_contract_visibility_party_idx on __rel_contract_visibility (party, contract_pk);
create index __rel_exercises_contract_idx on __rel_exercises using hash (event_pk);
create index __rel_tmp_lifecycle_ix_idx on __rel_tmp_lifecycle (archived_tx_ix);

insert into __rel_watermark (singleton) values (true) on conflict do nothing;
insert into __rel_pruning_metadata (singleton) values (true) on conflict do nothing;
