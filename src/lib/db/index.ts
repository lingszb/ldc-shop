import { sql } from '@vercel/postgres';
import { drizzle as drizzleVercel } from 'drizzle-orm/vercel-postgres';
import { drizzle as drizzlePg } from 'drizzle-orm/node-postgres';
import { Pool } from 'pg';
import * as schema from './schema';

let db: any;

// Use Vercel Postgres if running on Vercel
if (process.env.VERCEL) {
    db = drizzleVercel(sql, { schema });
} else {
    // Use standard Postgres (Docker/Self-hosted)
    const pool = new Pool({
        connectionString: process.env.POSTGRES_URL
    });
    db = drizzlePg(pool, { schema });
}

export { db };
