#!/bin/bash
# Ensure Airflow webserver binds to Render's provided port
export AIRFLOW__WEBSERVER__WEB_SERVER_PORT="${PORT}"
# Use simplest executor and point metadata to your Postgres if not already via env vars
# Start Airflow in standalone mode (includes db init, scheduler, webserver)
exec airflow standalone
