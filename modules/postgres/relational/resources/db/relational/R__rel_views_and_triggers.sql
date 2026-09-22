-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create or replace function __rel_current_writer() returns __rel_watermark.instance_id%type
as $$ select instance_id from __rel_watermark limit 1 $$
language sql;

create or replace procedure __rel_ensure_writer_valid() as
$$
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
$$ language plpgsql;

create or replace function __rel_update_watermark_fn() returns trigger as
$$
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
$$ language plpgsql;

drop trigger if exists __rel_update_watermark_trg on __rel_watermark;
create trigger __rel_update_watermark_trg
    before update of tx_ix
    on __rel_watermark
    for each row
execute function __rel_update_watermark_fn();

create or replace view transactions as
select t.tx_ix,
       t.ledger_offset,
       t.transaction_id,
       t.effective_at,
       t.synchronizer_id,
       t.workflow_id,
       t.external_transaction_hash,
       t.paid_traffic_cost
from __rel_transactions t
where t.ledger_offset between (select oldest_offset()) and (select latest_offset());

create or replace view active_contracts as
select c.contract_pk,
       c.contract_id,
       c.template_entity_pk,
       c.representative_package_id,
       c.created_tx_ix,
       c.created_at_offset,
       c.signatories,
       c.observers,
       c.creation_synchronizer_id
from __rel_contracts c
where c.life_ix @> (select latest_ix())
  and not c.divulged_only
  and c.redaction_id is null;

create or replace view reassignments as
select e.ledger_offset, e.node_id, e.tx_ix, e.contract_id, e.event_kind,
       r.reassignment_id, r.source_synchronizer_id, r.target_synchronizer_id,
       r.submitter, r.reassignment_counter, r.assignment_exclusivity
from __query_events e join __rel_reassignments r using (event_pk)
where e.ledger_offset between (select oldest_offset()) and (select latest_offset());
