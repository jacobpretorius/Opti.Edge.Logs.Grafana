import { ContainerClient } from '@azure/storage-blob';
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
const sasUrl = process.env.EDGE_LOGS_BLOB_URL;
const lokiUrl = process.env.LOKI_URL || 'http://loki:3100';
const pollingInterval = 15000; // Poll every 15 seconds
const tempLogDir = process.env.TEMP_LOG_DIR || '/usr/src/app/logs_temp';
const LOKI_PUSH_API = `${lokiUrl}/loki/api/v1/push`;
const BATCH_SIZE = 200;

// --- State ---
let containerClient;
let containerNameForLabels = 'cloudflarelogpush';
let lastProcessedFileEndTime = null; // Track the END time of the last successfully processed file
let checkInProgress = false;
let pollTimeoutId = null;

// --- Initial Checks & Setup ---
if (!sasUrl) {
  console.error('EDGE_LOGS_BLOB_URL must be provided in .env');
  process.exit(1);
}
try {
  containerClient = new ContainerClient(sasUrl);
  const urlPath = new URL(sasUrl).pathname;
  const pathParts = urlPath.split('/').filter(part => part.length > 0);
  if (pathParts.length > 0) containerNameForLabels = pathParts[0];
  console.log(`ContainerClient created for container: ${containerNameForLabels}`);
} catch (error) {
  console.error('Failed to create ContainerClient:', error);
  process.exit(1);
}

const commonLabels = { job: 'cloudflare-edge-logs', container: containerNameForLabels };

// --- Helper Functions ---

/**
 * Parses Cloudflare log filename like 20250429T153213Z_20250429T153320Z_c80262f4.log.gz
 * Returns { startTime: Date, endTime: Date } or null if invalid format.
 */
function parseFilenameTimestamp(filename) {
  // Match timestamps at the beginning, ignoring the rest (hash, extension)
  const match = filename.match(/^(\d{8}T\d{6}Z)_(\d{8}T\d{6}Z)/);
  if (match && match[1] && match[2]) {
    try {
      // Reformat slightly for Date constructor (YYYY-MM-DDTHH:MM:SSZ)
      const startTimeStr = match[1].replace(/(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z/, '$1-$2-$3T$4:$5:$6Z');
      const endTimeStr = match[2].replace(/(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z/, '$1-$2-$3T$4:$5:$6Z');
      const startTime = new Date(startTimeStr);
      const endTime = new Date(endTimeStr);

      if (!isNaN(startTime) && !isNaN(endTime)) {
        // console.log(`Parsed ${filename}: Start=${startTime.toISOString()}, End=${endTime.toISOString()}`); // Debug log
        return { startTime, endTime };
      } else {
        console.warn(`Parsed invalid date from filename ${filename}: StartStr=${startTimeStr}, EndStr=${endTimeStr}`);
      }
    } catch (e) {
      console.warn(`Exception parsing timestamp from filename ${filename}:`, e);
    }
  }
  // console.warn(`Failed to match or parse filename format: ${filename}`); // Debug log
  return null;
}

async function downloadBlob(blobName, localPath) {
  const blobClient = containerClient.getBlobClient(blobName);
  try {
    await blobClient.downloadToFile(localPath);
    return true;
  } catch (error) {
    console.error(`Error downloading blob ${blobName}:`, error);
    try { await fs.unlink(localPath); } catch (e) { /* Ignore */ }
    return false;
  }
}

async function sendToLoki(logs) {
  if (logs.length === 0) return;
  const body = JSON.stringify({
    streams: [{
      stream: commonLabels,
      values: logs.map(log => [
        (new Date(log.EdgeStartTimestamp).getTime() * 1000000).toString(),
        JSON.stringify(log)
      ])
    }]
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
      return false; // Indicate failure
    }
    return true; // Indicate success
  } catch (error) {
    console.error(`Error in sendToLoki request for ${logs.length} logs:`, error);
    return false; // Indicate failure
  }
}

/**
 * Processes a single gzipped log file and returns true on success, false on failure.
 */
async function processSingleBlob(blobName) {
  console.log(`Attempting to process blob: ${blobName}`);
  const localPath = path.join(tempLogDir, blobName);
  let fileProcessedSuccessfully = false;

  const downloaded = await downloadBlob(blobName, localPath);
  if (!downloaded) {
    console.error(`Failed to download ${blobName}, cannot process.`);
    return false; // Cannot proceed if download failed
  }

  const baseName = path.basename(localPath, '.gz');
  const unzippedFilePath = path.join(tempLogDir, baseName);
  const logsBatch = [];
  let linesProcessed = 0;
  let allBatchesSentSuccessfully = true;

  try {
    // Decompress
    await new Promise((resolve, reject) => {
      const gunzip = createGunzip();
      const source = createReadStream(localPath);
      const destination = createWriteStream(unzippedFilePath);
      source.pipe(gunzip).pipe(destination)
        .on('finish', resolve)
        .on('error', (err) => {
          console.error(`Error decompressing ${path.basename(localPath)}:`, err);
          fs.unlink(localPath).catch(e => console.warn(`Cleanup failed for ${localPath}:`, e.message));
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
              const success = await sendToLoki([...logsBatch]);
              if (!success) allBatchesSentSuccessfully = false;
              logsBatch.length = 0;
            }
          } else if (logEntry) {
            console.warn(`Skipping log entry missing EdgeStartTimestamp in ${baseName}`);
          }
          callback();
        }))
        .on('finish', async () => {
          if (logsBatch.length > 0) {
            const success = await sendToLoki(logsBatch);
            if (!success) allBatchesSentSuccessfully = false;
          }
          console.log(`Finished reading ${linesProcessed} lines from ${baseName}. Send success: ${allBatchesSentSuccessfully}`);
          if (allBatchesSentSuccessfully) {
            fileProcessedSuccessfully = true;
          } else {
            console.error(`Failed to send some batches for ${baseName}. File considered failed.`);
          }
          resolve();
        })
        .on('error', (err) => {
          console.error(`Error reading/processing ${baseName}:`, err);
          resolve();
        });
    });

  } catch (error) {
    console.error(`Unexpected error during processing of ${path.basename(localPath)}:`, error);
  } finally {
    // Cleanup
    try { await fs.unlink(localPath); } catch (e) { if (e.code !== 'ENOENT') console.warn(`Could not delete ${path.basename(localPath)} (gz):`, e.message); }
    try { await fs.unlink(unzippedFilePath); } catch (e) { if (e.code !== 'ENOENT') console.warn(`Could not delete ${baseName} (unzipped):`, e.message); }
  }

  return fileProcessedSuccessfully;
}

/**
 * Lists blobs and finds the one with the latest end time based on filename.
 * Returns { name: string, endTime: Date } or null.
 */
async function findLatestBlobInfo() {
  let latestBlobName = null;
  let latestEndTime = new Date(0); // Start with epoch time

  console.log('Scanning for latest existing log file...');
  try {
    const blobs = containerClient.listBlobsFlat();
    for await (const blob of blobs) {
      if (blob.name.endsWith('.log.gz')) {
        const timestamps = parseFilenameTimestamp(blob.name);
        if (timestamps && timestamps.endTime > latestEndTime) {
          latestEndTime = timestamps.endTime;
          latestBlobName = blob.name;
        }
      }
    }
  } catch (error) {
    console.error('Error listing blobs during initial scan:', error);
    return null;
  }

  if (latestBlobName) {
    console.log(`Latest blob found: ${latestBlobName} (End Time: ${latestEndTime.toISOString()})`);
    return { name: latestBlobName, endTime: latestEndTime };
  } else {
    console.log('No suitable .log.gz files found in container.');
    return null;
  }
}

// --- Main Polling Loop ---

async function checkAzureAndProcessLogs() {
  if (checkInProgress) {
    console.log('Skipping check, previous run still in progress.');
    pollTimeoutId = setTimeout(checkAzureAndProcessLogs, pollingInterval);
    return;
  }
  checkInProgress = true;

  // Use epoch if no file has been successfully processed yet
  const checkStartTime = lastProcessedFileEndTime || new Date(0);

  console.log(`\n[${new Date().toISOString()}] Checking for logs started after ${checkStartTime.toISOString()}...`);
  let blobsToProcess = [];

  try {
    const blobs = containerClient.listBlobsFlat();
    for await (const blob of blobs) {
      if (blob.name.endsWith('.log.gz')) {
        const timestamps = parseFilenameTimestamp(blob.name);
        // Process if the file's START time is strictly after the END time of the last known good file
        if (timestamps && timestamps.startTime > checkStartTime) {
          // console.log(` > Found candidate: ${blob.name} (Start: ${timestamps.startTime.toISOString()})`); // Debug
          blobsToProcess.push({ name: blob.name, startTime: timestamps.startTime, endTime: timestamps.endTime });
        } else if (timestamps) {
          // console.log(` > Skipping old/processed: ${blob.name} (Start: ${timestamps.startTime.toISOString()})`); // Debug
        }
      }
    }

    if (blobsToProcess.length > 0) {
      blobsToProcess.sort((a, b) => a.startTime - b.startTime);
      console.log(`Found ${blobsToProcess.length} new log file(s) to process.`);

      for (const blobInfo of blobsToProcess) {
        const success = await processSingleBlob(blobInfo.name);
        if (success) {
          // Update state only on successful processing, using the END time of the processed file
          lastProcessedFileEndTime = blobInfo.endTime;
          console.log(`Successfully processed. New checkpoint time: ${lastProcessedFileEndTime.toISOString()} (from ${blobInfo.name})`);
        } else {
          console.error(`Failed to process ${blobInfo.name}. Stopping processing this cycle to avoid skipping files.`);
          break; // Maintain order, retry next cycle
        }
      }
    } else {
      console.log('No new log files found.');
    }

  } catch (error) {
    console.error('Error during Azure blob check/processing:', error);
    if (error.statusCode === 403) {
      console.error('Received a 403 Forbidden error. Check SAS token permissions (list/read).');
    }
  } finally {
    checkInProgress = false;
    // console.log(`Scheduling next check. Current checkpoint: ${lastProcessedFileEndTime?.toISOString() || 'None'}`); // Debug
    pollTimeoutId = setTimeout(checkAzureAndProcessLogs, pollingInterval);
  }
}

// --- Initial Startup Logic ---

async function initializeAndProcessLatest() {
  try {
    await fs.mkdir(tempLogDir, { recursive: true });
    console.log(`Temporary log directory ensured at: ${tempLogDir}`);
  } catch (error) {
    console.error(`Failed to create temporary log directory ${tempLogDir}:`, error);
    process.exit(1);
  }

  console.log(`Starting Log Ingestor Service...`);
  console.log(`Connecting to Azure Container: ${containerNameForLabels}`);
  console.log(`Sending logs to Loki: ${LOKI_PUSH_API}`);
  console.log(`Polling Interval: ${pollingInterval / 1000} seconds`);

  const latestBlobInfo = await findLatestBlobInfo();

  if (latestBlobInfo) {
    console.log(`Found latest existing blob: ${latestBlobInfo.name}. Attempting initial processing...`);
    // Process the latest file found on startup
    const success = await processSingleBlob(latestBlobInfo.name);
    if (success) {
      // Set the checkpoint only if the initial processing succeeded
      lastProcessedFileEndTime = latestBlobInfo.endTime;
      console.log(`Initial processing successful. Checkpoint set to: ${lastProcessedFileEndTime.toISOString()}`);
    } else {
      console.error(`Initial processing failed for ${latestBlobInfo.name}. Will start polling without an initial checkpoint.`);
      // lastProcessedFileEndTime remains null, the first poll will check from epoch
    }
  } else {
    console.log('No existing log files found. Will process files as they appear.');
    // lastProcessedFileEndTime remains null
  }

  // Start the regular polling loop regardless of initial processing outcome
  console.log('Starting regular polling...');
  checkAzureAndProcessLogs();
}

// --- Start the service ---
initializeAndProcessLatest(); // Use the updated startup function name

// --- Signal Handling ---
function shutdown() {
  console.log('Shutting down...');
  if (pollTimeoutId) {
    clearTimeout(pollTimeoutId);
    pollTimeoutId = null;
    console.log('Polling timer cleared.');
  }
  console.log('Exiting process.');
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown); 