CREATE KEYSPACE IF NOT EXISTS spark_streams
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE spark_streams;

CREATE TABLE IF NOT EXISTS created_users (
    id UUID PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    gender TEXT,
    address TEXT,
    post_code TEXT,
    email TEXT,
    username TEXT,
    dob TEXT,
    registered_date TEXT,
    phone TEXT,
    picture TEXT
); 