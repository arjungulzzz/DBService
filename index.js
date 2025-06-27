require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const fs = require('fs');
const morgan = require('morgan');
const compression = require('compression');
const zlib = require('zlib');

const app = express();
const port = process.env.PORT || 3001;

// Enable CORS for all origins (customize as needed)
app.use(cors());
app.use(express.json());
if (process.env.ENABLE_COMPRESSION === 'true') {
  app.use(compression());
}

// Postgres connection config (replace with your actual credentials)
const pool = new Pool({
  user: process.env.PGUSER,
  host: process.env.PGHOST,
  database: process.env.PGDATABASE,
  password: process.env.PGPASSWORD,
  port: process.env.PGPORT || 5432,
});

// Logging setup
const logStream = fs.createWriteStream(process.env.LOG_FILE || 'service.log', { flags: 'a' });
app.use(morgan('dev'));

// Placeholder POST endpoint
app.post('/query', async (req, res) => {
  const requestTime = new Date().toISOString();
  let arrivalLog = {
    event: 'request_arrived',
    time: requestTime,
    endpoint: '/query',
    remote_addr: req.ip || req.connection.remoteAddress,
    request: req.body
  };
  fs.appendFileSync(process.env.LOG_FILE || 'service.log', JSON.stringify(arrivalLog) + '\n');
  const apiStart = Date.now();
  let logDetails = {
    time: requestTime,
    endpoint: '/query',
    request: req.body,
    remote_addr: req.ip || req.connection.remoteAddress
  };
  let responseBody = null;
  let status = 200;
  let error = null;
  let rowCount = 0;
  let queryStr = '';
  let paramsArr = [];
  let earlyReturn = false;
  let compressed = false;
  let originalSize = 0;
  let compressedSize = 0;
  let sqlTime = 0;
  let compressionTime = 0;
  try {
    // Validation
    if (!req.body || (typeof req.body !== 'object')) {
      status = 400;
      error = 'Request body must be a JSON object.';
      responseBody = JSON.stringify({ error });
      res.status(status).json({ error });
      earlyReturn = true;
      return;
    }
    const hasInterval = typeof req.body.interval === 'string' && req.body.interval.trim().length > 0;
    const hasDateRange = req.body.dateRange && typeof req.body.dateRange === 'object' && typeof req.body.dateRange.from === 'string' && typeof req.body.dateRange.to === 'string';
    if (!hasInterval && !hasDateRange) {
      status = 400;
      error = 'Request must include either a valid "interval" string or a "dateRange" object with "from" and "to" ISO strings.';
      responseBody = JSON.stringify({ error });
      res.status(status).json({ error });
      earlyReturn = true;
      return;
    }

    let query = `
      SELECT ali.log_date_time, asli.host_name, asli.repository_path, asli.port_number, asli.version_number, asli.as_server_mode, asli.as_start_date_time, asli.as_server_config,
             ali.user_id, ali.report_id_name, ali.error_number, ali.xql_query_id, ali.log_message
      FROM as_log_info ali, as_start_log_info asli
      WHERE ali.as_instance_id = asli.as_instance_id
    `;
    let whereClauses = [];
    let params = [];
    if (hasInterval) {
      whereClauses.push(`ali.log_date_time > NOW() - INTERVAL '${req.body.interval.replace(/'/g, "''")}'`);
    } else if (hasDateRange) {
      whereClauses.push(`ali.log_date_time BETWEEN $${params.length + 1} AND $${params.length + 2}`);
      params.push(req.body.dateRange.from, req.body.dateRange.to);
    }
    if (whereClauses.length > 0) {
      query += ' AND ' + whereClauses.join(' AND ');
    }
    query += ' ORDER BY ali.log_date_time DESC';
    const sqlStart = Date.now();
    const result = await pool.query(query, params);
    sqlTime = Date.now() - sqlStart;
    responseBody = JSON.stringify(result.rows);
    rowCount = result.rowCount;
    queryStr = query.replace(/\s+/g, ' ');
    paramsArr = params;
    originalSize = Buffer.byteLength(responseBody || '', 'utf8');
    // Check if compression is enabled and client accepts gzip
    if (process.env.ENABLE_COMPRESSION === 'true' && (req.headers['accept-encoding'] || '').includes('gzip')) {
      const compressionStart = Date.now();
      compressed = true;
      compressedSize = zlib.gzipSync(responseBody).length;
      compressionTime = Date.now() - compressionStart;
      res.set('X-Compressed', 'true');
    } else {
      res.set('X-Compressed', 'false');
    }
    res.json(result.rows);
  } catch (err) {
    status = 500;
    error = err.message;
    responseBody = JSON.stringify({ error: err.message });
    originalSize = Buffer.byteLength(responseBody || '', 'utf8');
    if (process.env.ENABLE_COMPRESSION === 'true' && (req.headers['accept-encoding'] || '').includes('gzip')) {
      const compressionStart = Date.now();
      compressed = true;
      compressedSize = zlib.gzipSync(responseBody).length;
      compressionTime = Date.now() - compressionStart;
      res.set('X-Compressed', 'true');
    } else {
      res.set('X-Compressed', 'false');
    }
    if (!earlyReturn) res.status(500).json({ error: err.message });
  } finally {
    const apiTotalTime = Date.now() - apiStart;
    logDetails.status = status;
    logDetails.api_total_time_ms = apiTotalTime;
    logDetails.sql_time_ms = sqlTime;
    logDetails.compression_time_ms = compressionTime;
    logDetails.duration_ms = apiTotalTime; // for backward compatibility
    logDetails.row_count = rowCount;
    logDetails.query = queryStr;
    logDetails.params = paramsArr;
    logDetails.response_size = originalSize;
    logDetails.compressed = compressed;
    logDetails.compressed_size = compressed ? compressedSize : undefined;
    if (error) logDetails.error = error;
    fs.appendFileSync(process.env.LOG_FILE || 'service.log', JSON.stringify(logDetails) + '\n');
  }
});

app.listen(port, () => {
  console.log(`Service listening on port ${port}`);
});
