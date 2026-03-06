import { Pool } from "pg";

declare global {
  var __permitsPool: Pool | undefined;
}

export const pool =
  global.__permitsPool ??
  new Pool({
    connectionString: process.env.DATABASE_URL,
    max: 10
  });

if (process.env.NODE_ENV !== "production") {
  global.__permitsPool = pool;
}
