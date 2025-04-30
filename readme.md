# Grafana + Loki + Optimizely Edge Logs

Streaming Cloudflare Edge Logs from Optimizely DXP to a self hosted Grafana + Loki instance.

## Setup

Create an `/.env` file based on the example from `/example.env` and replace with your values where needed. 

Also, consider changing the default Grapaha username and password while you are at it.

## Running Locally

docker compose up --build

http://localhost:3000

Login as user / pass configured, or default admin/admin

## Dashboard Me

You can import the JSON dashboard from my example with `/example-dashboard.json` using the Gradana UI. Give it a couple of minutes on initial startup to get enough data to fill all the panels.