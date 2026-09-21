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
begin
    -- Bypass the writer check when instance_id is being updated explicitly, so a writer reset can invalidate its predecessor.
    if new.instance_id = old.instance_id then
        call __rel_ensure_writer_valid();
    end if;

    if new.tx_ix is null or new.ledger_offset is null then
        raise exception '__rel_watermark.tx_ix and __rel_watermark.ledger_offset must not be null';
    end if;

    with drained as (
        delete from __rel_tmp_lifecycle where archived_tx_ix <= new.tx_ix
            returning contract_id, archived_tx_ix, archived_at_offset)
    update __rel_contracts c
    set archived_tx_ix     = d.archived_tx_ix,
        archived_at_offset = d.archived_at_offset
    from drained d
    where c.contract_id = d.contract_id and c.archived_tx_ix is null;

    return new;
end;
$$ language plpgsql;

drop trigger if exists __rel_update_watermark_trg on __rel_watermark;
create trigger __rel_update_watermark_trg
    before update of tx_ix
    on __rel_watermark
    for each row
execute function __rel_update_watermark_fn();
