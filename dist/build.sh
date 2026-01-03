#!/bin/bash

cat ../async.sql > scripts/async_client_full.sql
cat ../async.sql ../async_server.sql ../async_ui.sql > scripts/async_server_full.sql

cp scripts/async_client_full.sql extension/client/async_client--1.0.sql
cp scripts/async_server_full.sql extension/server/async--1.0.sql
