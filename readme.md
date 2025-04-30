# Optimizely Edge Logs + Grafana + Loki

Streaming [Cloudflare Edge Logs from Optimizely DXP](https://docs.developers.optimizely.com/content-management-system/docs/logging-options#edge-logs-streaming-using-cdn-beta) to a self-hosted Grafana + Loki instance.

![tech stack](https://github.com/jacobpretorius/Opti.Edge.Logs.Grafana/blob/main/images/tech.png)

## Setup & Running

1. Clone this repo
2. Create an `/.env` file based on the example from [example.env](https://github.com/jacobpretorius/Opti.Edge.Logs.Grafana/blob/main/example.env), replace with your values where needed. Consider changing the default Grafana username and password while you are at it.
3. Open a terminal in this directory and start the docker stack `docker compose up --build`
4. Grafana should now be running at [http://localhost:3000](http://localhost:3000)
5. Login as user / pass configured, or default admin/admin
6. Shutdown with `docker compose down`

## Dashboard

You can import the JSON dashboard from my example with [example-dashboard.json](https://github.com/jacobpretorius/Opti.Edge.Logs.Grafana/blob/main/example-dashboard.json) using the Grafana UI. Give it a couple of minutes on initial startup to get enough data to fill all the panels.

![example dashboard](https://github.com/jacobpretorius/Opti.Edge.Logs.Grafana/blob/main/images/example-dashboard.png)

## Privacy

This project doesn't export or share your edge logs or DXP API keys or anything like that with me or anyone else. Feel free to inspect [/log-ingestor/index.js](https://github.com/jacobpretorius/Opti.Edge.Logs.Grafana/blob/main/log-ingestor/index.js) or any of the other files to confirm that yourself.
