-- Explicit rebuild-all only, after every storage user has drained. Keep schema
-- and the current rebuild Operation, deleted separately with a bound parameter.
DELETE FROM chunk_text_index;
DELETE FROM graph_entity_edges;
DELETE FROM graph_entity_mentions;
DELETE FROM colbert_windows;
DELETE FROM chunk_dense_vectors;
DELETE FROM chunk_projections;
DELETE FROM retrieval_projections;
DELETE FROM semantic_annotations;
DELETE FROM annotation_memo;
DELETE FROM unit_relationships;
DELETE FROM content_units;
DELETE FROM query_execution_records;
DELETE FROM forensic_snapshots;
DELETE FROM sync_queue;
DELETE FROM acquisition_records;
DELETE FROM source_locations;
DELETE FROM parse_runs;
DELETE FROM source_objects;
DELETE FROM system_events;
DELETE FROM policy_versions;
