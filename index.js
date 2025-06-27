require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const fs = require('fs');
const morgan = require('morgan');

const app = express();
const port = process.env.PORT || 3001;

// Enable CORS for all origins (customize as needed)
app.use(cors());
app.use(express.json());

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
  const start = Date.now();
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
      // Only basic validation: must be a non-empty string, assume frontend sends valid Postgres interval
      whereClauses.push(`ali.log_date_time > NOW() - INTERVAL '${req.body.interval.replace(/'/g, "''")}'`);
    } else if (hasDateRange) {
      whereClauses.push(`ali.log_date_time BETWEEN $${params.length + 1} AND $${params.length + 2}`);
      params.push(req.body.dateRange.from, req.body.dateRange.to);
    }
    if (whereClauses.length > 0) {
      query += ' AND ' + whereClauses.join(' AND ');
    }
    query += ' ORDER BY ali.log_date_time DESC';
    const result = await pool.query(query, params);
    responseBody = JSON.stringify(result.rows);
    rowCount = result.rowCount;
    queryStr = query.replace(/\s+/g, ' ');
    paramsArr = params;
    res.json(result.rows);
  } catch (err) {
    status = 500;
    error = err.message;
    responseBody = JSON.stringify({ error: err.message });
    if (!earlyReturn) res.status(500).json({ error: err.message });
  } finally {
    const duration = Date.now() - start;
    logDetails.status = status;
    logDetails.duration_ms = duration;
    logDetails.row_count = rowCount;
    logDetails.query = queryStr;
    logDetails.params = paramsArr;
    logDetails.response_size = Buffer.byteLength(responseBody || '', 'utf8');
    if (error) logDetails.error = error;
    fs.appendFileSync(process.env.LOG_FILE || 'service.log', JSON.stringify(logDetails) + '\n');
  }
});

app.listen(port, () => {
  console.log(`Service listening on port ${port}`);
});
