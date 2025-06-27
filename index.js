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
    // After query and params are finalized, add them to logDetails
    logDetails.query = query;
    logDetails.query_params = params;
    logDetails.input_params = req.body;
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
    logDetails.row_count = rowCount;
    logDetails.response_size = originalSize;
    logDetails.compressed = compressed;
    logDetails.compressed_size = compressed ? compressedSize : undefined;
    if (error) logDetails.error = error;
    fs.appendFileSync(process.env.LOG_FILE || 'service.log', JSON.stringify(logDetails) + '\n');
  }
});

app.post('/v1/logs', async (req, res) => {
  const requestTime = new Date().toISOString();
  let arrivalLog = {
    event: 'request_arrived',
    time: requestTime,
    endpoint: '/v1/logs',
    remote_addr: req.ip || req.connection.remoteAddress,
    request: req.body
  };
  fs.appendFileSync(process.env.LOG_FILE || 'service.log', JSON.stringify(arrivalLog) + '\n');
  const apiStart = Date.now();
  let logDetails = {
    time: requestTime,
    endpoint: '/v1/logs',
    request: req.body,
    remote_addr: req.ip || req.connection.remoteAddress
  };
  let status = 200;
  let error = null;
  let sqlTime = 0;
  let compressionTime = 0;
  let responseBody = null;
  let compressed = false;
  let originalSize = 0;
  let compressedSize = 0;
  let totalCount = 0;
  let logs = [];
  let chartData = [];
  let groupData = [];
  try {
    // Parse and validate request
    const { dateRange, interval, pagination, sort, filters, groupBy, chartBreakdownBy } = req.body || {};
    let whereClauses = ['ali.as_instance_id = asli.as_instance_id'];
    let params = [];
    // Date filtering
    if (interval) {
      whereClauses.push(`ali.log_date_time > NOW() - INTERVAL '${interval.replace(/'/g, "''")}'`);
    } else if (dateRange && dateRange.from && dateRange.to) {
      whereClauses.push(`ali.log_date_time BETWEEN $${params.length + 1} AND $${params.length + 2}`);
      params.push(dateRange.from, dateRange.to);
    }
    // Additional filters
    if (filters && typeof filters === 'object') {
      Object.entries(filters).forEach(([key, value]) => {
        if (value !== undefined && value !== null && value !== '') {
          whereClauses.push(`ali.${key} = $${params.length + 1}`);
          params.push(value);
        }
      });
    }
    // Sorting
    let orderBy = 'ali.log_date_time DESC';
    if (sort && sort.column) {
      const dir = (sort.direction && sort.direction.toLowerCase() === 'ascending') ? 'ASC' : 'DESC';
      orderBy = `ali.${sort.column} ${dir}`;
    }
    // Pagination
    let limit = 100, offset = 0;
    if (pagination && pagination.pageSize) {
      limit = parseInt(pagination.pageSize, 10) || 100;
    }
    if (pagination && pagination.page) {
      offset = ((parseInt(pagination.page, 10) - 1) * limit) || 0;
    }
    // Main logs query
    let baseQuery = `FROM as_log_info ali, as_start_log_info asli WHERE ${whereClauses.join(' AND ')}`;
    let logsQuery = `SELECT ali.*, asli.host_name, asli.repository_path, asli.port_number, asli.version_number, asli.as_server_mode, asli.as_start_date_time, asli.as_server_config FROM ${baseQuery} ORDER BY ${orderBy} LIMIT $${params.length + 1} OFFSET $${params.length + 2}`;
    let countQuery = `SELECT COUNT(*) FROM ${baseQuery}`;
    // Aggregation queries
    let groupDataQuery = (groupBy && groupBy !== 'none') ? `SELECT ali.${groupBy} as group, COUNT(*) as count FROM ${baseQuery} GROUP BY ali.${groupBy}` : null;
    let chartDataQuery = (chartBreakdownBy && chartBreakdownBy !== 'none') ? `SELECT ali.${chartBreakdownBy} as breakdown, COUNT(*) as count FROM ${baseQuery} GROUP BY ali.${chartBreakdownBy}` : null;
    // Query execution
    const sqlStart = Date.now();
    const [logsResult, countResult, groupResult, chartResult] = await Promise.all([
      pool.query(logsQuery, [...params, limit, offset]),
      pool.query(countQuery, params),
      groupDataQuery ? pool.query(groupDataQuery, params) : Promise.resolve({ rows: [] }),
      chartDataQuery ? pool.query(chartDataQuery, params) : Promise.resolve({ rows: [] })
    ]);
    sqlTime = Date.now() - sqlStart;
    logs = logsResult.rows.map(row => ({ id: row.id || row.log_id || row.as_instance_id + '-' + row.log_date_time, ...row }));
    totalCount = parseInt(countResult.rows[0].count, 10);
    groupData = groupResult.rows;
    chartData = chartResult.rows;
    // Response
    responseBody = JSON.stringify({ logs, totalCount, groupData, chartData });
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
    // After query and params are finalized, add them to logDetails
    logDetails.query = logsQuery;
    logDetails.query_params = [...params, limit, offset];
    logDetails.input_params = req.body;
    // After query and params are finalized, log the SQL being executed
    const sqlLog = {
      event: 'sql_executed',
      time: new Date().toISOString(),
      endpoint: req.originalUrl,
      remote_addr: req.ip || req.connection.remoteAddress,
      query: logsQuery || query, // logsQuery for /v1/logs, query for /query
      query_params: [...(params || []), ...(typeof limit !== 'undefined' ? [limit] : []), ...(typeof offset !== 'undefined' ? [offset] : [])],
      input_params: req.body
    };
    fs.appendFileSync(process.env.LOG_FILE || 'service.log', JSON.stringify(sqlLog) + '\n');
    // Remove query and query_params from logDetails for the final log
    delete logDetails.query;
    delete logDetails.query_params;
    res.json({ logs, totalCount, groupData, chartData });
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
    res.status(500).json({ error: err.message });
  } finally {
    const apiTotalTime = Date.now() - apiStart;
    logDetails.status = status;
    logDetails.api_total_time_ms = apiTotalTime;
    logDetails.sql_time_ms = sqlTime;
    logDetails.compression_time_ms = compressionTime;
    logDetails.row_count = logs.length;
    logDetails.total_count = totalCount;
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
