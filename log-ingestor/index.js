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

// --- Configuration ---
const sasUrl = process.env.EDGE_LOGS_BLOB_URL; // Use the SAS URL
const lokiUrl = process.env.LOKI_URL || 'http://loki:3100';
const pollingInterval = parseInt(process.env.POLLING_INTERVAL_MS || '60000', 10);
const tempLogDir = process.env.TEMP_LOG_DIR || '/usr/src/app/logs_temp'; // Use the path inside the container
const LOKI_PUSH_API = `${lokiUrl}/loki/api/v1/push`;
const BATCH_SIZE = 200; // Log batch size for sending to Loki

// --- State ---
const serviceStartTime = new Date(); // Record startup time (UTC)
let containerClient;
let containerNameForLabels = 'cloudflarelogpush';
let processedBlobs = new Set(); // Still useful to prevent reprocessing within a single poll cycle
let checkInProgress = false;
let pollTimeoutId = null; // Variable to store the timeout ID

// --- Initial Checks & Setup ---
if (!sasUrl) {
  console.error('Azure Storage SAS URL (EDGE_LOGS_BLOB_URL) must be provided in the log-ingestor/.env file.');
  process.exit(1);
}

try {
  containerClient = new ContainerClient(sasUrl);
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

// --- Helper Functions (downloadBlob, sendToLoki, processLogFile - mostly unchanged) ---

async function downloadBlob(blobName, localPath) {
  const blobClient = containerClient.getBlobClient(blobName);
  try {
    // console.log(`Downloading blob: ${blobName} to ${localPath}`); // More concise logging now
    await blobClient.downloadToFile(localPath);
    // console.log(`Downloaded ${blobName}`);
    return true;
  } catch (error) {
    console.error(`Error downloading blob ${blobName}:`, error);
    try { await fs.unlink(localPath); } catch (e) { /* Ignore cleanup error */ }
    return false;
  }
}

async function sendToLoki(logs) {
  if (logs.length === 0) return;
  const body = JSON.stringify({
    streams: [
      {
        stream: { job: 'cloudflare-edge-logs', container: containerNameForLabels },
        values: logs.map(log => [
          (new Date(log.EdgeStartTimestamp).getTime() * 1000000).toString(),
          JSON.stringify(log)
        ])
      }
    ]
  });
  try {
    const response = await fetch(LOKI_PUSH_API, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: body
    });
    if (!response.ok) {
      const errorText = await response.text();
      console.error(`Error sending ${logs.length} logs to Loki: ${response.status} ${response.statusText}`, errorText);
    } // else { console.log(`Sent ${logs.length} logs to Loki.`); }
  } catch (error) {
    console.error(`Error in sendToLoki request for ${logs.length} logs:`, error);
  }
}

async function processLogFile(gzippedFilePath) {
  const baseName = path.basename(gzippedFilePath, '.gz');
  const unzippedFilePath = path.join(tempLogDir, baseName);
  const logsBatch = [];
  let linesProcessed = 0;

  console.log(`Processing ${path.basename(gzippedFilePath)}...`);

  try {
    // Decompress
    await new Promise((resolve, reject) => {
      const gunzip = createGunzip();
      const source = createReadStream(gzippedFilePath);
      const destination = createWriteStream(unzippedFilePath);
      source.pipe(gunzip).pipe(destination)
        .on('finish', resolve)
        .on('error', (err) => {
          console.error(`Error decompressing ${path.basename(gzippedFilePath)}:`, err);
          fs.unlink(gzippedFilePath).catch(e => console.warn(`Cleanup failed for ${gzippedFilePath}:`, e.message));
          reject(err);
        });
    });

    // Read, Parse, Batch Send
    await new Promise((resolve) => {
      createReadStream(unzippedFilePath)
        .pipe(split2(line => {
          try {
            if (line.trim() === '') return undefined;
            return JSON.parse(line);
          } catch (e) {
            console.warn(`Skipping invalid JSON line in ${baseName}: ${line.substring(0, 100)}...`, e.message);
            return undefined;
          }
        }))
        .pipe(through2.obj(async (logEntry, enc, callback) => {
          if (logEntry?.EdgeStartTimestamp) {
            linesProcessed++;
            logsBatch.push(logEntry);
            if (logsBatch.length >= BATCH_SIZE) {
              await sendToLoki([...logsBatch]);
              logsBatch.length = 0;
            }
          } else if (logEntry) {
            console.warn(`Skipping log entry missing EdgeStartTimestamp in ${baseName}`);
          }
          callback();
        }))
        .on('finish', async () => {
          if (logsBatch.length > 0) {
            await sendToLoki(logsBatch);
          }
          console.log(`Finished processing ${linesProcessed} lines from ${baseName}`);
          resolve();
        })
        .on('error', (err) => {
          console.error(`Error reading/processing ${baseName}:`, err);
          resolve(); // Proceed to finally block for cleanup
        });
    });

  } catch (error) {
    console.error(`Unexpected error during processing of ${path.basename(gzippedFilePath)}:`, error);
  } finally {
    // Cleanup
    try { await fs.unlink(gzippedFilePath); } catch (e) { if (e.code !== 'ENOENT') console.warn(`Could not delete ${path.basename(gzippedFilePath)} (gz):`, e.message); }
    try { await fs.unlink(unzippedFilePath); } catch (e) { if (e.code !== 'ENOENT') console.warn(`Could not delete ${baseName} (unzipped):`, e.message); }
  }
}


// --- Main Loop --- Adjusted to check lastModified time ---

async function checkAzureAndProcessLogs() {
  if (checkInProgress) {
    console.log('Skipping check, previous run still in progress.');
    setTimeout(checkAzureAndProcessLogs, pollingInterval);
    return;
  }
  checkInProgress = true;

  console.log(`\n[${new Date().toISOString()}] Checking Azure Blob Storage for new logs (since ${serviceStartTime.toISOString()})...`);
  let blobsProcessedInThisRun = 0;
  let blobsFound = 0;
  let blobsConsidered = 0;

  try {
    // Use listBlobsFlat with includeMetadata option if needed, but lastModified is usually default
    const blobs = containerClient.listBlobsFlat({ includeMetadata: false }); // lastModified is in properties
    for await (const blob of blobs) {
      blobsFound++;
      // Check if blob was modified after the service started
      if (blob.properties.lastModified >= serviceStartTime) {
        blobsConsidered++;
        // Basic check: Is it a .log.gz file and not already processed in this specific polling cycle?
        if (blob.name.endsWith('.log.gz') && !processedBlobs.has(blob.name)) {
          console.log(`Found new blob: ${blob.name} (Modified: ${blob.properties.lastModified.toISOString()})`);
          const localPath = path.join(tempLogDir, blob.name);
          const downloaded = await downloadBlob(blob.name, localPath);
          if (downloaded) {
            await processLogFile(localPath); // Handles cleanup
            processedBlobs.add(blob.name); // Mark as processed for this session
            blobsProcessedInThisRun++;
          }
        } else if (processedBlobs.has(blob.name)) {
          // Already processed in this poll cycle (edge case)
        } else if (!blob.name.endsWith('.log.gz')) {
          // console.log(`Skipping non-log file: ${blob.name}`);
        }
      } // else { // Blob is older than service start time - uncomment to debug
      // console.log(`Skipping old blob: ${blob.name} (Modified: ${blob.properties.lastModified.toISOString()})`);
      // }
    }

    // Clear the processed set after *each* successful polling cycle.
    // This allows files modified again later to be reprocessed if needed,
    // while still preventing reprocessing within the *same* cycle.
    processedBlobs.clear();

    if (blobsFound === 0) {
      console.log('No blobs found in the container.');
    } else if (blobsConsidered === 0) {
      console.log(`No blobs modified since startup out of ${blobsFound} total.`);
    } else if (blobsProcessedInThisRun === 0) {
      console.log(`No *new* log files to process out of ${blobsConsidered} blobs modified since startup.`);
    } else {
      console.log(`Finished processing ${blobsProcessedInThisRun} new log file(s).`);
    }

  } catch (error) {
    console.error('Error during Azure blob check/processing:', error);
    if (error.statusCode === 403) {
      console.error('Received a 403 Forbidden error. Check SAS token permissions (list/read).');
    }
    // Don't clear processedBlobs on error, might retry successfully next time
  } finally {
    checkInProgress = false; // Release the lock
    console.log(`Scheduling next check in ${pollingInterval / 1000} seconds.`);
    // Store the timeout ID
    pollTimeoutId = setTimeout(checkAzureAndProcessLogs, pollingInterval);
  }
}

// --- Initial Setup & Start ---

(async () => {
  // Ensure temp directory exists
  try {
    await fs.mkdir(tempLogDir, { recursive: true });
    console.log(`Temporary log directory ensured at: ${tempLogDir}`);
  } catch (error) {
    console.error(`Failed to create temporary log directory ${tempLogDir}:`, error);
    process.exit(1);
  }

  console.log(`Starting Log Ingestor Service at ${serviceStartTime.toISOString()} ...`);
  console.log(`Connecting to Azure Container via SAS URL (Container: ${containerNameForLabels})`);
  console.log(`Sending logs to Loki: ${LOKI_PUSH_API}`);
  console.log(`Polling Interval: ${pollingInterval / 1000} seconds`);
  console.log('Will only process blobs modified after startup time.');

  // Start the first check immediately
  checkAzureAndProcessLogs();
})();

// --- Signal Handling ---
function shutdown() {
  console.log('Shutting down...');
  // Clear timeout using the stored ID
  if (pollTimeoutId) {
    clearTimeout(pollTimeoutId);
    pollTimeoutId = null; // Reset the ID
    console.log('Polling timer cleared.');
  }
  // Add more cleanup if needed (e.g., wait for ongoing uploads)
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown); 