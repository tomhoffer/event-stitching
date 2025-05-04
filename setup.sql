CREATE TABLE profiles (id SERIAL PRIMARY KEY, cookie varchar(4096), message_id varchar(1024), phone varchar(14));

CREATE TABLE events (
    id SERIAL PRIMARY KEY,
    event_id SMALLINT,
    event_timestamp TIMESTAMP,
    identifiers JSONB,
    processed BOOLEAN DEFAULT FALSE
);


CREATE INDEX idx_profiles_phone ON profiles(phone);
CREATE INDEX idx_profiles_message_id ON profiles(message_id);
CREATE INDEX idx_profiles_cookie ON profiles(cookie);

CREATE INDEX idx_events_processed_timestamp ON events(processed, event_timestamp);
