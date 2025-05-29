FROM node:22-alpine

WORKDIR /usr/src/app

# Install tzdata and set timezone to UTC
RUN apk add --no-cache tzdata
ENV TZ=UTC

COPY package*.json ./

RUN npm install

COPY . .

# Create the temporary logs directory within the container
# Ensure this path matches TEMP_LOG_DIR env var and volume mount
RUN mkdir -p /usr/src/app/logs_temp

# Define LOKI_URL and EDGE_LOGS_BLOB_URL in .env file or as environment variables
ENV LOKI_URL=http://loki:3100

# Default temp directory inside container
ENV TEMP_LOG_DIR=/usr/src/app/logs_temp

CMD [ "npm", "start" ] 