import { BlobServiceClient, ContainerClient } from '@azure/storage-blob';
import dotenv from 'dotenv';
import fetch from 'node-fetch';
import { createGunzip } from 'zlib';
import { createReadStream, createWriteStream, promises as fs } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import split2 from 'split2';
import through2 from 'through2';

// Resolve __dirname for ES Modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

dotenv.config({ path: path.resolve(__dirname, '.env') }); // Load .env from log-ingestor directory

const sasUrl = process.env.EDGE_LOGS_BLOB_URL; // Use the SAS URL
const lokiUrl = process.env.LOKI_URL || 'http://loki:3100';
const pollingInterval = parseInt(process.env.POLLING_INTERVAL_MS || '60000', 10);
const tempLogDir = process.env.TEMP_LOG_DIR || '/usr/src/app/logs_temp'; // Use the path inside the container

if (!sasUrl) {
  console.error('Azure Storage SAS URL (EDGE_LOGS_BLOB_URL) must be provided in the log-ingestor/.env file.');
  process.exit(1);
}

// Create ContainerClient directly from SAS URL
let containerClient;
let containerNameForLabels = 'cloudflarelogpush'; // Default label if name extraction fails
try {
  containerClient = new ContainerClient(sasUrl);
  // Attempt to extract container name from SAS URL path for labeling (optional, might be fragile)
  const urlPath = new URL(sasUrl).pathname;
  const pathParts = urlPath.split('/').filter(part => part.length > 0);
  if (pathParts.length > 0) {
    containerNameForLabels = pathParts[0];
  }
  console.log(`Successfully created ContainerClient for container: ${containerNameForLabels}`);
} catch (error) {
  console.error('Failed to create ContainerClient from SAS URL. Ensure EDGE_LOGS_BLOB_URL is valid:', error);
  process.exit(1);
}


const LOKI_PUSH_API = `${lokiUrl}/loki/api/v1/push`;

// --- Helper Functions ---

async function downloadBlob(blobName, localPath) {
  // ContainerClient is already initialized with SAS auth
  const blobClient = containerClient.getBlobClient(blobName);
  try {
    console.log(`Downloading blob: ${blobName} to ${localPath}`);
    await blobClient.downloadToFile(localPath);
    console.log(`Downloaded ${blobName}`);
    return true;
  } catch (error) {
    console.error(`Error downloading blob ${blobName}:`, error);
    // Attempt to clean up potentially partial download
    try { await fs.unlink(localPath); } catch (e) { /* Ignore cleanup error */ }
    return false;
  }
}

async function sendToLoki(logs) {
  if (logs.length === 0) return;

  const body = JSON.stringify({
    streams: [
      {
        stream: {
          // Add labels relevant to your logs
          job: 'cloudflare-edge-logs',
          container: containerNameForLabels // Use extracted container name
        },
        values: logs.map(log => [
          // Use EdgeStartTimestamp for accurate timing, convert to nanoseconds
          (new Date(log.EdgeStartTimestamp).getTime() * 1000000).toString(),
          JSON.stringify(log) // Send the full JSON object as the log line
        ])
      }
    ]
  });

  try {
    // console.log(`Sending ${logs.length} logs to Loki...`); // Uncomment for debugging
    const response = await fetch(LOKI_PUSH_API, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: body
    });

    if (!response.ok) {
      const errorText = await response.text();
      console.error(`Error sending logs to Loki: ${response.status} ${response.statusText}`, errorText);
    } else {
      // console.log(`Successfully sent ${logs.length} logs to Loki.`); // Uncomment for debugging
    }
  } catch (error) {
    console.error('Error sending request to Loki:', error);
  }
}

async function processLogFile(gzippedFilePath) {
  const baseName = path.basename(gzippedFilePath, '.gz');
  // Ensure the unzipped path is also within the designated temp directory
  const unzippedFilePath = path.join(tempLogDir, baseName);
  const logsBatch = [];
  const BATCH_SIZE = 200; // Adjust batch size as needed

  console.log(`Processing ${gzippedFilePath}...`);

  try {
    // Decompress
    await new Promise((resolve, reject) => {
      const gunzip = createGunzip();
      const source = createReadStream(gzippedFilePath);
      const destination = createWriteStream(unzippedFilePath);

      source.pipe(gunzip).pipe(destination)
        .on('finish', () => {
          console.log(`Decompressed to ${unzippedFilePath}`);
          resolve();
        })
        .on('error', (err) => {
          console.error(`Error decompressing ${gzippedFilePath}:`, err);
          // Clean up gzipped file if decompression fails
          fs.unlink(gzippedFilePath).catch(e => console.warn(`Cleanup failed for ${gzippedFilePath}:`, e.message));
          reject(err);
        });
    });

    // Read line by line, parse JSON, and batch send to Loki
    await new Promise((resolve, reject) => {
      createReadStream(unzippedFilePath)
        .pipe(split2(line => {
          try {
            // Ignore empty lines
            if (line.trim() === '') return undefined;
            return JSON.parse(line);
          } catch (e) {
            console.warn(`Skipping invalid JSON line: ${line}`, e);
            return undefined; // Skip lines that are not valid JSON
          }
        }))
        .pipe(through2.obj(async (logEntry, enc, callback) => {
          if (logEntry && logEntry.EdgeStartTimestamp) { // Basic validation
            logsBatch.push(logEntry);
            if (logsBatch.length >= BATCH_SIZE) {
              await sendToLoki([...logsBatch]); // Send a copy
              logsBatch.length = 0; // Clear the batch
            }
          } else if (logEntry) { // Log entry exists but missing timestamp
            console.warn('Skipping log entry missing EdgeStartTimestamp:', JSON.stringify(logEntry).substring(0, 200) + '...');
          }
          // If logEntry was undefined (empty line or parse error), just continue
          callback();
        }))
        .on('finish', async () => {
          // Send any remaining logs in the last batch
          if (logsBatch.length > 0) {
            await sendToLoki(logsBatch);
          }
          console.log(`Finished processing ${unzippedFilePath}`);
          resolve();
        })
        .on('error', (err) => {
          console.error(`Error reading or processing ${unzippedFilePath}:`, err);
          // Don't reject immediately, allow finally block to clean up
          resolve(); // Resolve to proceed to finally block
        });
    });

  } catch (error) {
    // Errors during decompression or reading should have been caught and logged
    // This catch block might catch unexpected errors
    console.error(`Unexpected error during processing of ${gzippedFilePath}:`, error);
  } finally {
    // Cleanup: Delete both gzipped and unzipped files
    try {
      await fs.unlink(gzippedFilePath);
      console.log(`Deleted temporary file: ${gzippedFilePath}`);
    } catch (e) {
      if (e.code !== 'ENOENT') { // Ignore if file already deleted
        console.warn(`Could not delete temporary file ${gzippedFilePath}:`, e.message);
      }
    }
    try {
      await fs.unlink(unzippedFilePath);
      console.log(`Deleted temporary file: ${unzippedFilePath}`);
    } catch (e) {
      // It might not exist if decompression or processing failed early
      if (e.code !== 'ENOENT') {
        console.warn(`Could not delete temporary file ${unzippedFilePath}:`, e.message);
      }
    }
  }
}


// --- Main Loop ---

// Keep track of processed blobs to avoid reprocessing in the same run
let processedBlobs = new Set();
let checkInProgress = false; // Prevent overlapping checks

async function checkAzureAndProcessLogs() {
  if (checkInProgress) {
    console.log('Skipping check, previous run still in progress.');
    // Ensure next check is still scheduled
    setTimeout(checkAzureAndProcessLogs, pollingInterval);
    return;
  }
  checkInProgress = true;

  console.log(`\n[${new Date().toISOString()}] Checking Azure Blob Storage for new logs...`);
  let blobsProcessedInThisRun = 0;
  let blobsFound = 0;

  try {
    const blobs = containerClient.listBlobsFlat();
    for await (const blob of blobs) {
      blobsFound++;
      // Basic check: Is it a .log.gz file and not already processed?
      if (blob.name.endsWith('.log.gz') && !processedBlobs.has(blob.name)) {
        const localPath = path.join(tempLogDir, blob.name);
        const downloaded = await downloadBlob(blob.name, localPath);
        if (downloaded) {
          await processLogFile(localPath); // This handles cleanup of the local file
          processedBlobs.add(blob.name); // Mark as processed for this run/session
          blobsProcessedInThisRun++;
          // Note: We don't delete the blob from Azure here, as per requirements.
        } else {
          // Optionally add blob.name to processedBlobs even on download failure
          // to prevent retrying a known bad file repeatedly in this session.
          // processedBlobs.add(blob.name);
        }
      } else if (processedBlobs.has(blob.name)) {
        // console.log(`Skipping already processed blob: ${blob.name}`); // Uncomment for debugging
      } else if (!blob.name.endsWith('.log.gz')) {
        // console.log(`Skipping non-log file: ${blob.name}`); // Uncomment for debugging
      }
    }

    if (blobsFound === 0) {
      console.log('No blobs found in the container.');
    } else if (blobsProcessedInThisRun === 0) {
      console.log(`No new log files found out of ${blobsFound} total blobs.`);
    } else {
      console.log(`Finished processing ${blobsProcessedInThisRun} new log file(s).`);
      // Optional: Clear the set if you want to reprocess files if they reappear (e.g., for testing)
      // processedBlobs.clear();
    }

  } catch (error) {
    console.error('Error during Azure blob check/processing:', error);
    if (error.statusCode === 403) {
      console.error('Received a 403 Forbidden error. Check if the SAS token is valid and has list/read permissions.');
    }
  } finally {
    checkInProgress = false; // Release the lock
    // Schedule the next check
    console.log(`Scheduling next check in ${pollingInterval / 1000} seconds.`);
    setTimeout(checkAzureAndProcessLogs, pollingInterval);
  }
}

// --- Initial Setup & Start ---

(async () => {
  // Ensure temp directory exists
  try {
    // Use the absolute path within the container defined by ENV
    await fs.mkdir(tempLogDir, { recursive: true });
    console.log(`Temporary log directory ensured at: ${tempLogDir}`);
  } catch (error) {
    console.error(`Failed to create temporary log directory ${tempLogDir}:`, error);
    process.exit(1);
  }

  console.log('Starting Log Ingestor Service...');
  console.log(`Connecting to Azure Container via SAS URL (Container: ${containerNameForLabels})`);
  console.log(`Sending logs to Loki: ${LOKI_PUSH_API}`);
  console.log(`Polling Interval: ${pollingInterval / 1000} seconds`);

  // Start the first check immediately
  checkAzureAndProcessLogs();
})();

// Basic signal handling for graceful shutdown (optional)
process.on('SIGINT', () => {
  console.log('Received SIGINT. Shutting down...');
  // Add cleanup logic here if needed
  process.exit(0);
});
process.on('SIGTERM', () => {
  console.log('Received SIGTERM. Shutting down...');
  // Add cleanup logic here if needed
  process.exit(0);
}); 