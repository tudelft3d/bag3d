#!/bin/bash
set -e

psql -p 5432 -h localhost -U postgres -d postgres -c "create role bag3d_tester with login password 'bag3d_test';"

psql -p 5432 -h localhost -U postgres -d postgres -c "create database bag3d_db with owner bag3d_tester;"

psql -p 5432 -h localhost -U postgres -d bag3d_db -c "create extension postgis;"

psql -p 5432 -h localhost -U postgres -d bag3d_db -f bag3d_db.backup
