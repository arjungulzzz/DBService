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
// app.post('/query', async (req, res) => {
//   const requestTime = new Date().toISOString();
//   let arrivalLog = {
//     event: 'request_arrived',
//     time: requestTime,
//     endpoint: '/query',
//     remote_addr: req.ip || req.connection.remoteAddress,
//     request: req.body
//   };
//   fs.appendFileSync(process.env.LOG_FILE || 'service.log', JSON.stringify(arrivalLog) + '\n');
//   const apiStart = Date.now();
//   let logDetails = {
//     time: requestTime,
//     endpoint: '/query',
//     request: req.body,
//     remote_addr: req.ip || req.connection.remoteAddress
//   };
//   let responseBody = null;
//   let status = 200;
//   let error = null;
//   let rowCount = 0;
//   let queryStr = '';
//   let paramsArr = [];
//   let earlyReturn = false;
//   let compressed = false;
//   let originalSize = 0;
//   let compressedSize = 0;
//   let sqlTime = 0;
//   let compressionTime = 0;
//   try {
//     // Validation
//     if (!req.body || (typeof req.body !== 'object')) {
//       status = 400;
//       error = 'Request body must be a JSON object.';
//       responseBody = JSON.stringify({ error });
//       res.status(status).json({ error });
//       earlyReturn = true;
//       return;
//     }
//     const hasInterval = typeof req.body.interval === 'string' && req.body.interval.trim().length > 0;
//     const hasDateRange = req.body.dateRange && typeof req.body.dateRange === 'object' && typeof req.body.dateRange.from === 'string' && typeof req.body.dateRange.to === 'string';
//     if (!hasInterval && !hasDateRange) {
//       status = 400;
//       error = 'Request must include either a valid "interval" string or a "dateRange" object with "from" and "to" ISO strings.';
//       responseBody = JSON.stringify({ error });
//       res.status(status).json({ error });
//       earlyReturn = true;
//       return;
//     }

//     let query = `
//       SELECT ali.log_date_time, asli.host_name, asli.repository_path, asli.port_number, asli.version_number, asli.as_server_mode, asli.as_start_date_time, asli.as_server_config,
//              ali.user_id, ali.report_id_name, ali.error_number, ali.xql_query_id, ali.log_message
//       FROM as_log_info ali, as_start_log_info asli
//       WHERE ali.as_instance_id = asli.as_instance_id
//     `;
//     let whereClauses = [];
//     let params = [];
//     if (hasInterval) {
//       whereClauses.push(`ali.log_date_time > NOW() - INTERVAL '${req.body.interval.replace(/'/g, "''")}'`);
//     } else if (hasDateRange) {
//       whereClauses.push(`ali.log_date_time BETWEEN $${params.length + 1} AND $${params.length + 2}`);
//       params.push(req.body.dateRange.from, req.body.dateRange.to);
//     }
//     if (whereClauses.length > 0) {
//       query += ' AND ' + whereClauses.join(' AND ');
//     }
//     query += ' ORDER BY ali.log_date_time DESC';
//     // After query and params are finalized, add them to logDetails
//     logDetails.query = query;
//     logDetails.query_params = params;
//     logDetails.input_params = req.body;
//     const sqlStart = Date.now();
//     const result = await pool.query(query, params);
//     sqlTime = Date.now() - sqlStart;
//     responseBody = JSON.stringify(result.rows);
//     rowCount = result.rowCount;
//     queryStr = query.replace(/\s+/g, ' ');
//     paramsArr = params;
//     originalSize = Buffer.byteLength(responseBody || '', 'utf8');
//     // Check if compression is enabled and client accepts gzip
//     if (process.env.ENABLE_COMPRESSION === 'true' && (req.headers['accept-encoding'] || '').includes('gzip')) {
//       const compressionStart = Date.now();
//       compressed = true;
//       compressedSize = zlib.gzipSync(responseBody).length;
//       compressionTime = Date.now() - compressionStart;
//       res.set('X-Compressed', 'true');
//     } else {
//       res.set('X-Compressed', 'false');
//     }
//     res.json(result.rows);
//   } catch (err) {
//     status = 500;
//     error = err.message;
//     responseBody = JSON.stringify({ error: err.message });
//     originalSize = Buffer.byteLength(responseBody || '', 'utf8');
//     if (process.env.ENABLE_COMPRESSION === 'true' && (req.headers['accept-encoding'] || '').includes('gzip')) {
//       const compressionStart = Date.now();
//       compressed = true;
//       compressedSize = zlib.gzipSync(responseBody).length;
//       compressionTime = Date.now() - compressionStart;
//       res.set('X-Compressed', 'true');
//     } else {
//       res.set('X-Compressed', 'false');
//     }
//     if (!earlyReturn) res.status(500).json({ error: err.message });
//   } finally {
//     const apiTotalTime = Date.now() - apiStart;
//     logDetails.status = status;
//     logDetails.api_total_time_ms = apiTotalTime;
//     logDetails.sql_time_ms = sqlTime;
//     logDetails.compression_time_ms = compressionTime;
//     logDetails.row_count = rowCount;
//     logDetails.response_size = originalSize;
//     logDetails.compressed = compressed;
//     logDetails.compressed_size = compressed ? compressedSize : undefined;
//     if (error) logDetails.error = error;
//     fs.appendFileSync(process.env.LOG_FILE || 'service.log', JSON.stringify(logDetails) + '\n');
//   }
// });

// Whitelisted columns for sorting and grouping
const ALI_COLUMNS = [
  'log_date_time', 'user_id', 'report_id_name', 'error_number', 'xql_query_id', 'log_message'
];
const ASLI_COLUMNS = [
  'host_name', 'repository_path', 'port_number', 'version_number', 'as_server_mode', 'as_start_date_time', 'as_server_config'
];
const ALL_COLUMNS = [
  'ali.log_date_time', 'asli.host_name', 'asli.repository_path', 'asli.port_number', 'asli.version_number', 'asli.as_server_mode', 'asli.as_start_date_time', 'asli.as_server_config',
  'ali.user_id', 'ali.report_id_name', 'ali.error_number', 'ali.xql_query_id', 'ali.log_message'
];

function isWhitelisted(col) {
  return ALI_COLUMNS.includes(col) || ASLI_COLUMNS.includes(col);
}

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
  let params = [];
  let paramIdx = 1;
  let limit = 100, offset = 0;
  try {
    const { dateRange, interval, pagination, sort, filters, groupBy, chartBreakdownBy } = req.body || {};
    // Build WHERE clause
    let whereClauses = [];
    if (interval) {
      whereClauses.push(`ali.log_date_time >= NOW() - CAST($${paramIdx} AS INTERVAL)`);
      params.push(interval);
      paramIdx++;
    } else if (dateRange && dateRange.from && dateRange.to) {
      whereClauses.push(`ali.log_date_time BETWEEN $${paramIdx} AND $${paramIdx + 1}`);
      params.push(dateRange.from, dateRange.to);
      paramIdx += 2;
    }
    if (filters && typeof filters === 'object') {
      for (const [key, value] of Object.entries(filters)) {
        if (value !== undefined && value !== null && value !== '') {
          if (ASLI_COLUMNS.includes(key)) {
            whereClauses.push(`asli.${key} ILIKE $${paramIdx}`);
            params.push(`${value}%`);
          } else if (ALI_COLUMNS.includes(key)) {
            if (key === 'error_number') {
              whereClauses.push(`ali.${key} = CAST($${paramIdx} AS INTEGER)`);
              params.push(value);
            } else {
              whereClauses.push(`ali.${key} ILIKE $${paramIdx}`);
              params.push(`${value}%`);
            }
          }
          paramIdx++;
        }
      }
    }
    // Sorting
    let orderBy = 'ali.log_date_time DESC';
    if (sort && isWhitelisted(sort.column)) {
      const dir = (sort.direction && sort.direction.toLowerCase() === 'ascending') ? 'ASC' : 'DESC';
      if (ASLI_COLUMNS.includes(sort.column)) {
        orderBy = `asli.${sort.column} ${dir}`;
      } else {
        orderBy = `ali.${sort.column} ${dir}`;
      }
    }
    // Pagination
    if (pagination && pagination.pageSize) {
      limit = parseInt(pagination.pageSize, 10) || 100;
    }
    if (pagination && pagination.page) {
      offset = ((parseInt(pagination.page, 10) - 1) * limit) || 0;
    }
    // Base query
    let baseFrom = 'FROM as_log_info ali JOIN as_start_log_info asli ON ali.as_instance_id = asli.as_instance_id';
    let where = whereClauses.length ? `WHERE ${whereClauses.join(' AND ')}` : '';
    // Main logs query
    let logsQuery = `SELECT ${ALL_COLUMNS.join(', ')} ${baseFrom} ${where} ORDER BY ${orderBy} LIMIT $${paramIdx} OFFSET $${paramIdx + 1}`;
    let logsParams = [...params, limit, offset];
    // totalCount query
    let countQuery = `SELECT COUNT(*) ${baseFrom} ${where}`;
    // groupData query
    let groupDataQuery = (groupBy && isWhitelisted(groupBy)) ? `SELECT ${ASLI_COLUMNS.includes(groupBy) ? `asli.${groupBy}` : `ali.${groupBy}`} as key, COUNT(*) as count ${baseFrom} ${where} GROUP BY key ORDER BY count DESC` : null;
    // chartData query (advanced: time buckets and breakdown)
    let chartDataQuery = null;
    if (chartBreakdownBy && isWhitelisted(chartBreakdownBy)) {
      chartDataQuery = `WITH TimeBuckets AS (
        SELECT
          date_trunc('hour', ali.log_date_time AT TIME ZONE 'UTC') as date,
          ${(ASLI_COLUMNS.includes(chartBreakdownBy) ? `asli.${chartBreakdownBy}` : `ali.${chartBreakdownBy}`)}::text as breakdown_key,
          COUNT(*) as error_count
        ${baseFrom} ${where}
        GROUP BY 1, 2
      )
      SELECT
        date::text,
        SUM(error_count)::integer as count,
        jsonb_object_agg(breakdown_key, error_count ORDER BY error_count DESC) as breakdown
      FROM TimeBuckets
      GROUP BY date
      ORDER BY date`;
    }
    // Log SQL
    const sqlLog = {
      event: 'sql_executed',
      time: new Date().toISOString(),
      endpoint: req.originalUrl,
      remote_addr: req.ip || req.connection.remoteAddress,
      query: logsQuery,
      query_params: logsParams,
      input_params: req.body
    };
    fs.appendFileSync(process.env.LOG_FILE || 'service.log', JSON.stringify(sqlLog) + '\n');
    // Query execution
    const sqlStart = Date.now();
    const [logsResult, countResult, groupResult, chartResult] = await Promise.all([
      pool.query(logsQuery, logsParams),
      pool.query(countQuery, params),
      groupDataQuery ? pool.query(groupDataQuery, params) : Promise.resolve({ rows: [] }),
      chartDataQuery ? pool.query(chartDataQuery, params) : Promise.resolve({ rows: [] })
    ]);
    sqlTime = Date.now() - sqlStart;
    logs = logsResult.rows;
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
    logDetails.input_params = req.body;
    if (error) logDetails.error = error;
    fs.appendFileSync(process.env.LOG_FILE || 'service.log', JSON.stringify(logDetails) + '\n');
  }
});

app.listen(port, () => {
  console.log(`Service listening on port ${port}`);
});
