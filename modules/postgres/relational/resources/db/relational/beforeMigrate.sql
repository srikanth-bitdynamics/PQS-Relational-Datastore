-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- A hook that runs before any Flyway migration. Adopt the current writer instance so migrations that touch
-- the watermark are not rejected by the writer fence.
do $$
begin
  if exists (
    select 1 from information_schema.columns
    where table_name = '__rel_watermark' and column_name = 'instance_id' and table_schema = current_schema()
  ) then
    update __rel_watermark set instance_id = current_setting('scribe.instance');
  end if;
end $$;
