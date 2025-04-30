# Optimizely Edge Logs + Grafana + Loki

Streaming Cloudflare Edge Logs from Optimizely DXP to a self hosted Grafana + Loki instance.

## Setup & Running

1. Clone this repo
2. Create an `/.env` file based on the example from `/example.env`, replace with your values where needed. Consider changing the default Grapaha username and password while you are at it.
3. Open a terminal in this directory and start the docker stack `docker compose up --build`
4. Graphana should now be running at [http://localhost:3000](http://localhost:3000)
5. Login as user / pass configured, or default admin/admin
6. Shutdown with `docker compose down`

## Dashboard

You can import the JSON dashboard from my example with `/example-dashboard.json` using the Gradana UI. Give it a couple of minutes on initial startup to get enough data to fill all the panels, or you may need to go into the panel that doesn't show data and click "Run query" for it to populate 🤷‍♂️.

## Privacy

This project doesn't export or share your edge logs or DXP API keys or anything like that with me or anyone else. Feel free to inspect `/log-ingestor/index.js` or any of the other files to confirm that yourself.