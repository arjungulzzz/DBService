# DBService

This is a minimal Node.js Express backend service that exposes a single POST endpoint (`/query`) to trigger SQL queries on a Postgres database and return results as JSON. CORS is enabled for integration with a Next.js frontend.

## Setup
1. Install dependencies:
   ```sh
   npm install
   ```
2. Update Postgres credentials in `index.js`.
3. Start the server:
   ```sh
   node index.js
   ```

## Endpoint
- **POST** `/query`
  - Request body: (to be defined)
  - Response: JSON array of query results

## Notes
- Update the SQL query and request parsing logic in `index.js` as needed.
- CORS is enabled for all origins by default.
