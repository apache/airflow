// To 0.7
alter table task_instance add column queue varchar(50) NULL;
alter table task_instance add column pool varchar(50) NULL;
alter table task_instance add column priority_weight INT NULL;
create index ti_pool on task_instance (pool, state) using btree;
// To 1.3
alter table known_events add column event_tags TEXT(200) NULL;