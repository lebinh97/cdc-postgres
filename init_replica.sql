-- Runs once on READ_REPLICA — subscribes to production's CDC stream
-- Tables are created automatically via the subscription's initial snapshot

CREATE SUBSCRIPTION cdc_sub
CONNECTION 'host=production port=5432 dbname=cdc_db user=admin password=admin123'
PUBLICATION cdc_pub;
