CREATE TABLE profiles (cookie varchar(4096), message_id varchar(1024), phone varchar(14), stitched boolean DEFAULT false);

CREATE TABLE events (
    id SERIAL PRIMARY KEY,
    event_id SMALLINT,
    event_timestamp TIMESTAMP,
    identifiers JSONB,
    processed BOOLEAN DEFAULT FALSE
)

