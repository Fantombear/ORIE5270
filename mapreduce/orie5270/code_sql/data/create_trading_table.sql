CREATE TABLE trading (
    id serial primary key,
    A varchar,
    B varchar
);

CREATE TABLE quote(
    id serial primary key,
    A varchar,
    C varchar,
);

CREATE TABLE temporal_trading(
    id serial primary key,
    A varchar,
    B varchar
);

CREATE TABLE temporal_quote(
    id serial primary key,
    A varchar,
    C varchar
);
