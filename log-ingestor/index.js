import { ContainerClient } from '@azure/storage-blob';
import fetch from 'node-fetch';
import { createGunzip } from 'zlib';
import { createReadStream, createWriteStream, promises as fs } from 'fs';
import path from 'path';
import split2 from 'split2';
import through2 from 'through2';
import crypto from 'crypto';

// --- Configuration ---
const dxpProjectId = process.env.DXP_PROJECT_ID;
const dxpClientKey = process.env.DXP_CLIENT_KEY;
const dxpClientSecret = process.env.DXP_CLIENT_SECRET;
const dxpEnvironment = process.env.DXP_ENVIRONMENT || 'General';
const dxpStorageContainer = process.env.DXP_STORAGE_CONTAINER || 'cloudflarelogpush';

const lokiUrl = process.env.LOKI_URL || 'http://loki:3100';
const pollingInterval = process.env.POLLING_INTERVAL || 15000; // Poll every 15 seconds
const tempLogDir = process.env.TEMP_LOG_DIR || '/usr/src/app/logs_temp';
const LOKI_PUSH_API = `${lokiUrl}/loki/api/v1/push`;
const BATCH_SIZE = 200;
const SAS_EXPIRY_BUFFER_MS = 60 * 60 * 1000; // Refresh SAS link 1 hour before expiry

// --- State ---
let containerClient = null;
let containerNameForLabels = dxpStorageContainer || 'cloudflarelogpush';
let lastProcessedFileEndTime = null; // Track the END time of the last successfully processed file
let checkInProgress = false;
let pollTimeoutId = null;
let currentSasUrl = null;
let sasUrlExpiresOn = null;

// --- Initial Checks & Setup ---
if (!dxpProjectId || !dxpClientKey || !dxpClientSecret || !dxpEnvironment || !dxpStorageContainer) {
  console.error('DXP_PROJECT_ID, DXP_CLIENT_KEY, DXP_CLIENT_SECRET, DXP_ENVIRONMENT, and DXP_STORAGE_CONTAINER must be provided in .env');
  process.exit(1);
}

const commonLabels = { job: 'cloudflare-edge-logs', container: containerNameForLabels };

// --- Helper Functions ---

/**
 * Fetches a SAS Link from the DXP API.
 * Returns { sasLink: string, expiresOn: Date } or throws an error.
 */
async function getDxpSasLink(projectId, clientKey, clientSecret, environment, storageContainer) {
  const apiUrl = `https://paasportal.episerver.net/api/v1.0/projects/${projectId}/environments/${environment}/storagecontainers/${storageContainer}/saslink`;
  const method = 'POST';
  const url = new URL(apiUrl);
  const pathAndQuery = url.pathname + url.search; // Get path and query

  const body = JSON.stringify({
    RetentionHours: 24,
    Writable: false // Read-only access is sufficient
  });

  const timestamp = Date.now().toString();
  const nonce = crypto.randomUUID().replace(/-/g, ''); // Generate nonce

  // Calculate body hash (MD5 -> Base64)
  const bodyBytes = Buffer.from(body, 'utf8');
  const bodyHashBytes = crypto.createHash('md5').update(bodyBytes).digest();
  const hashBody = bodyHashBytes.toString('base64');

  // Create signature string
  const message = `${clientKey}${method}${pathAndQuery}${timestamp}${nonce}${hashBody}`;
  const messageBytes = Buffer.from(message, 'utf8');

  // Calculate signature (HMACSHA256 -> Base64)
  // DXP expects the secret to be base64 encoded already, so we decode it first.
  const hmac = crypto.createHmac('sha256', Buffer.from(clientSecret, 'base64'));
  const signatureHash = hmac.update(messageBytes).digest();
  const signature = signatureHash.toString('base64');

  // Construct Authorization header
  const authHeader = `epi-hmac ${clientKey}:${timestamp}:${nonce}:${signature}`;

  console.log('Requesting new SAS link from DXP API...');

  const controller = new AbortController();
  const timeoutMs = 30000; // 30 seconds timeout
  const timeoutId = setTimeout(() => {
    console.error(`DXP API request timed out after ${timeoutMs / 1000}s`);
    controller.abort();
  }, timeoutMs);

  try {
    const response = await fetch(apiUrl, {
      method: method,
      headers: {
        'Authorization': authHeader,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      },
      body: body,
      signal: controller.signal // Added abort signal
    });

    if (!response.ok) {
      const errorText = await response.text();
      console.error(`Failed to get DXP SAS Link: ${response.status} ${response.statusText}`, errorText.substring(0, 500));
      throw new Error(`Failed to get DXP SAS Link: ${response.status}`);
    }

    const data = await response.json();

    if (data.success && data.result?.sasLink && data.result?.expiresOn) {
      const expiresOn = new Date(data.result.expiresOn);
      console.log(`Successfully obtained new SAS link, expires: ${expiresOn.toISOString()}`);
      return { sasLink: data.result.sasLink, expiresOn: expiresOn };
    } else {
      console.error('Failed to parse SAS link or expiry from DXP API response:', JSON.stringify(data).substring(0, 500));
      throw new Error('Invalid response format from DXP API');
    }
  } catch (error) {
    console.error('Network or other error fetching DXP SAS Link:', error);
    throw error; // Re-throw to be handled by the caller
  } finally {
    clearTimeout(timeoutId);
  }
}

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
  if (!containerClient) {
    console.error(`Cannot download blob ${blobName}, containerClient is not initialized.`);
    return false;
  }
  const blobClient = containerClient.getBlobClient(blobName);
  try {
    await blobClient.downloadToFile(localPath);
    return true;
  } catch (error) {
    console.error(`Error downloading blob ${blobName}:`, error);
    if (error.statusCode === 403) {
      console.warn(`Received 403 downloading ${blobName}. SAS URL might be expired or invalid.`);
    }
    try { await fs.unlink(localPath); } catch (e) { /* Ignore */ }
    return false;
  }
}

async function sendToLoki(logs) {
  if (logs.length === 0) return true; // No logs to send is a success
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
      // Log Loki rejection clearly, but the overall function will still return success
      console.warn(`Loki rejected batch: ${response.status} ${response.statusText}`, errorText.substring(0, 500) + (errorText.length > 500 ? '...' : ''));
      return false; // Indicate Loki rejection for this batch
    }
    return true; // Indicate successful send for this batch
  } catch (error) {
    console.error(`Network error sending batch to Loki:`, error);
    return false; // Indicate failure
  }
}

/**
 * Processes a single gzipped log file.
 * Returns the file's endTime (Date object) if the file was fully read,
 * even if some batches failed to send to Loki.
 * Returns null if a critical error prevented reading the file (e.g., download/decompression).
 */
async function processSingleBlob(blobInfo) {
  const { name: blobName, endTime: blobEndTime } = blobInfo;
  console.log(`Attempting to process blob: ${blobName}`);
  const localPath = path.join(tempLogDir, blobName);
  let fileReadCompleted = false;

  const downloaded = await downloadBlob(blobName, localPath);
  if (!downloaded) {
    console.error(`Failed to download ${blobName}, cannot process.`);
    return null; // Critical error (potentially includes auth error handled above)
  }

  const baseName = path.basename(localPath, '.gz');
  const unzippedFilePath = path.join(tempLogDir, baseName);
  const logsBatch = [];
  let linesProcessed = 0;
  let anyBatchFailed = false; // Track if *any* batch send failed

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
          reject(err); // Reject promise on decompression error
        });
    });

    // Read, Parse, Batch Send
    await new Promise((resolve) => {
      let streamErrored = false;
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
          if (streamErrored) return callback(); // Stop processing if stream errored
          if (logEntry?.EdgeStartTimestamp) {
            linesProcessed++;
            logsBatch.push(logEntry);
            if (logsBatch.length >= BATCH_SIZE) {
              const success = await sendToLoki([...logsBatch]);
              if (!success) anyBatchFailed = true; // Logged in sendToLoki
              logsBatch.length = 0;
            }
          } else if (logEntry) {
            console.warn(`Skipping log entry missing EdgeStartTimestamp in ${baseName}`);
          }
          callback();
        }))
        .on('finish', async () => {
          if (streamErrored) {
            console.warn(`Stream finished after error for ${baseName}. Some data might be missing.`);
            resolve();
            return;
          }
          if (logsBatch.length > 0) {
            const success = await sendToLoki(logsBatch);
            if (!success) anyBatchFailed = true; // Logged in sendToLoki
          }
          console.log(`Finished reading ${linesProcessed} lines from ${baseName}.`);
          fileReadCompleted = true; // Mark as read fully
          if (anyBatchFailed) {
            console.warn(`Some batches failed to send for ${baseName}, but file read completed.`);
          }
          resolve();
        })
        .on('error', (err) => {
          streamErrored = true;
          console.error(`Error reading/processing stream for ${baseName}:`, err);
          resolve(); // Resolve anyway to allow cleanup
        });
    });

  } catch (error) {
    // This catches critical errors like decompression failure
    console.error(`Critical error processing ${path.basename(localPath)}:`, error);
    fileReadCompleted = false; // Ensure this isn't marked as completed
  } finally {
    // Cleanup
    try { await fs.unlink(localPath); } catch (e) { if (e.code !== 'ENOENT') console.warn(`Could not delete ${path.basename(localPath)} (gz):`, e.message); }
    try { await fs.unlink(unzippedFilePath); } catch (e) { if (e.code !== 'ENOENT') console.warn(`Could not delete ${baseName} (unzipped):`, e.message); }
  }

  // Return the blob's end time if we successfully read the whole file,
  // otherwise return null to indicate a failure before completion.
  return fileReadCompleted ? blobEndTime : null;
}

/**
 * Lists blobs and finds the one with the latest end time based on filename.
 * Returns { name: string, endTime: Date } or null.
 */
async function findLatestBlobInfo() {
  if (!containerClient) {
    console.error('Cannot find latest blob, containerClient is not initialized.');
    return null;
  }
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
    if (error.statusCode === 403) {
      console.warn(`Received 403 listing blobs. SAS URL might be expired or invalid.`);
    }
    return null; // Indicate failure to list
  }

  if (latestBlobName) {
    console.log(`Latest blob found: ${latestBlobName} (End Time: ${latestEndTime.toISOString()})`);
    return { name: latestBlobName, endTime: latestEndTime };
  } else {
    console.log('No suitable .log.gz files found in container.');
    return null;
  }
}

/**
 * Ensures the SAS URL is valid, refreshing if necessary.
 * Returns true if the SAS URL is valid (or was successfully refreshed), false otherwise.
 */
async function ensureValidSasUrl() {
  const now = Date.now();
  // Check if SAS URL exists and is nearing expiry
  if (!currentSasUrl || !sasUrlExpiresOn || sasUrlExpiresOn.getTime() < now + SAS_EXPIRY_BUFFER_MS) {
    if (sasUrlExpiresOn) {
      console.log(`SAS URL expiring soon (expires ${sasUrlExpiresOn.toISOString()}) or is missing. Attempting refresh...`);
    } else {
      console.log(`SAS URL missing. Attempting initial fetch...`); // Should only happen if initial fetch failed
    }

    try {
      const { sasLink, expiresOn } = await getDxpSasLink(
        dxpProjectId,
        dxpClientKey,
        dxpClientSecret,
        dxpEnvironment,
        dxpStorageContainer
      );
      currentSasUrl = sasLink;
      sasUrlExpiresOn = expiresOn;
      // Recreate the client with the new URL
      containerClient = new ContainerClient(currentSasUrl);
      // Update containerNameForLabels based on DXP Storage Container name
      containerNameForLabels = dxpStorageContainer;
      // Update commonLabels in case the container name changed
      commonLabels.container = containerNameForLabels;
      console.log(`ContainerClient updated with new SAS URL for container: ${containerNameForLabels}`);
      return true; // Refresh successful
    } catch (error) {
      console.error('Failed to refresh/obtain SAS URL. Cannot proceed with Azure operations.', error);
      // Invalidate client if refresh fails
      containerClient = null;
      currentSasUrl = null;
      sasUrlExpiresOn = null;
      return false; // Refresh failed
    }
  }
  // Existing SAS URL is still valid
  return true;
}

/**
 * Lists blobs from the container client with a specified timeout.
 * Returns an array of blob items or throws an error if timeout or listing fails.
 */
async function listBlobsWithTimeout(client, timeoutMs) {
  let timeoutHandle;
  const timeoutPromise = new Promise((_, reject) => {
    timeoutHandle = setTimeout(() => {
      reject(new Error(`Blob listing timed out after ${timeoutMs / 1000}s`));
    }, timeoutMs);
  });

  try {
    const blobs = [];
    const iterator = client.listBlobsFlat(); // Get the async iterator
    // Race the actual listing against the timeout
    const resultPromise = (async () => {
      for await (const blob of iterator) {
        blobs.push(blob);
      }
      return blobs;
    })();

    const result = await Promise.race([resultPromise, timeoutPromise]);
    clearTimeout(timeoutHandle); // Clear timeout if listing finishes or errors first
    return result; // This will be the array of blobs if successful
  } catch (error) {
    clearTimeout(timeoutHandle); // Ensure timeout is cleared on any error
    throw error; // Re-throw the error (either from listing or the timeout itself)
  }
}

// --- Main Polling Loop ---

async function checkAzureAndProcessLogs() {
  if (checkInProgress) {
    console.log('Skipping check, previous run still in progress.');
    return;
  }
  checkInProgress = true;

  try {
    // 1. Ensure SAS URL is valid before interacting with Azure
    const sasOk = await ensureValidSasUrl();
    if (!sasOk) {
      console.error("SAS URL is invalid and could not be refreshed. Skipping Azure check this cycle.");
      // No finally block here, so ensure checkInProgress is reset and next poll is scheduled
      checkInProgress = false;
      if (pollTimeoutId) clearTimeout(pollTimeoutId);
      pollTimeoutId = setTimeout(checkAzureAndProcessLogs, pollingInterval);
      return;
    }

    // We should have a valid containerClient now
    if (!containerClient) {
      console.error("Container client is unexpectedly null after SAS check. Skipping cycle.");
      checkInProgress = false;
      if (pollTimeoutId) clearTimeout(pollTimeoutId);
      pollTimeoutId = setTimeout(checkAzureAndProcessLogs, pollingInterval);
      return;
    }

    const checkStartTime = lastProcessedFileEndTime || new Date(0);
    console.log(`\n[${new Date().toISOString()}] Checking for logs started after ${checkStartTime.toISOString()}...`);
    let blobsToProcess = [];
    const BLOB_LISTING_TIMEOUT_MS = 60000; // 60 seconds timeout for listing blobs

    // 2. List Blobs
    try {
      // Use the new function with a timeout
      const listedBlobs = await listBlobsWithTimeout(containerClient, BLOB_LISTING_TIMEOUT_MS);

      for (const blob of listedBlobs) {
        if (blob.name.endsWith('.log.gz')) {
          const timestamps = parseFilenameTimestamp(blob.name);
          // Process blobs whose *start* time is >= our checkpoint (last *end* time)
          if (timestamps && timestamps.startTime >= checkStartTime) {
            blobsToProcess.push({ name: blob.name, startTime: timestamps.startTime, endTime: timestamps.endTime });
          }
        }
      }
    } catch (error) {
      console.error('Error during Azure blob check/listing:', error.message); // Log error.message for more clarity
      if (error.message && error.message.includes('timed out')) {
        console.warn(`Blob listing timed out after ${BLOB_LISTING_TIMEOUT_MS / 1000}s. Will attempt SAS refresh on next cycle.`);
      } else if (error.statusCode === 403) {
        console.warn('Received a 403 Forbidden error listing blobs. SAS URL might be invalid. Will attempt refresh on next cycle.');
      } else {
        // Generic message for other types of errors during listing
        console.warn('An unexpected error occurred during blob listing. Will attempt SAS refresh on next cycle.');
      }
      // Forcing SAS refresh for timeout or other listing errors is a good precaution.
      sasUrlExpiresOn = new Date(0); // Force expiry check on next run
      // Don't process blobs if listing failed
      blobsToProcess = []; // Ensure blobsToProcess is empty to prevent processing
    }

    // 3. Process Found Blobs
    if (blobsToProcess.length > 0) {
      blobsToProcess.sort((a, b) => a.startTime - b.startTime);
      console.log(`Found ${blobsToProcess.length} new log file(s) to process.`);

      for (const blobInfo of blobsToProcess) {
        // processSingleBlob implicitly uses the potentially updated containerClient
        const processedEndTime = await processSingleBlob(blobInfo);

        if (processedEndTime !== null) {
          // Success: File read completed (even if Loki rejected some batches).
          // Update checkpoint to the end time of the file we just finished reading.
          lastProcessedFileEndTime = processedEndTime;
          console.log(`Read completed for ${blobInfo.name}. New checkpoint time: ${lastProcessedFileEndTime.toISOString()}`);
        } else {
          // Failure: Critical error occurred (download, decompress, stream read error).
          console.error(`Critical error processing ${blobInfo.name}. Stopping processing this cycle to maintain order.`);
          // If the download failed with 403, ensureValidSasUrl should handle refresh next cycle.
          break; // Stop processing further blobs this cycle
        }
      }
    } else {
      console.log('No new log files found or listing failed.');
    }

  } catch (error) {
    // Catch unexpected errors in the main loop logic itself
    console.error('Unexpected error in main processing loop:', error);
  } finally {
    checkInProgress = false;
    if (pollTimeoutId) clearTimeout(pollTimeoutId);
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
  console.log(`Using DXP Project: ${dxpProjectId}, Env: ${dxpEnvironment}, Container: ${dxpStorageContainer}`);
  console.log(`Sending logs to Loki: ${LOKI_PUSH_API}`);
  console.log(`Polling Interval: ${pollingInterval / 1000} seconds`);

  // 1. Initial SAS URL Fetch
  const initialSasOk = await ensureValidSasUrl(); // This will fetch and set currentSasUrl, sasUrlExpiresOn, containerClient
  if (!initialSasOk) {
    console.error("Failed to obtain initial SAS URL from DXP API. Cannot start processing. Exiting.");
    process.exit(1);
  }

  // 2. Process Latest Existing Blob (Optional, using the fetched SAS URL)
  const latestBlobInfo = await findLatestBlobInfo(); // Uses the containerClient created by ensureValidSasUrl

  if (latestBlobInfo) {
    console.log(`Found latest existing blob: ${latestBlobInfo.name}. Attempting initial processing...`);
    // Process the latest file found on startup
    const processedEndTime = await processSingleBlob(latestBlobInfo); // Uses the same containerClient
    if (processedEndTime !== null) {
      // Set the checkpoint only if the initial processing succeeded
      lastProcessedFileEndTime = processedEndTime;
      console.log(`Initial processing successful. Checkpoint set to: ${lastProcessedFileEndTime.toISOString()}`);
    } else {
      console.error(`Initial processing failed for ${latestBlobInfo.name}. Will start polling without an initial checkpoint.`);
      // lastProcessedFileEndTime remains null, the first poll will check from epoch
    }
  } else {
    console.log('No existing log files found or listing failed. Will process files as they appear.');
    // lastProcessedFileEndTime remains null
  }

  // 3. Start the regular polling loop regardless of initial processing outcome
  console.log('Starting regular polling...');
  // Clear any potentially existing timer before starting the main loop
  if (pollTimeoutId) clearTimeout(pollTimeoutId);
  checkAzureAndProcessLogs(); // This will immediately check expiry and list blobs
}

// --- Start the service ---
initializeAndProcessLatest();

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