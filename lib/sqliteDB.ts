import fs from 'fs';
import path from 'path';
import { DatabaseSync } from 'node:sqlite';

const BUSY_TIMEOUT = 10_000;
const WRITE_RETRIES = 3;
const RETRY_DELAY = 250;

const sleep = (ms: number) => Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);

function isLocked(e: unknown): boolean {
	if (!e || typeof e !== 'object') return false;
	const err = e as { errcode?: number; errstr?: string; message?: string };
	if (err.errcode === 5 || err.errcode === 6) return true;
	return /database is (locked|busy)|SQLITE_(BUSY|LOCKED)/i.test(`${err.errstr || ''} ${err.message || ''}`);
}

/**
 * lowdb adapter backed by SQLite.
 * Stores the whole `db.data` object across a `kv(collection, key, value)` table,
 * one row per entry (no giant JSON file rewritten on every save).
 * If `jsonFile` is given and the table is empty, existing JSON data is imported once.
 */
class SQLiteAdapter {
	db: DatabaseSync;
	private _walWarned = false;

	constructor(file: string, jsonFiles?: string | string[]) {
		this.db = new DatabaseSync(path.resolve(file));
		this.db.exec(`PRAGMA busy_timeout = ${BUSY_TIMEOUT}`);
		this._ensureWal();
		this.db.exec('CREATE TABLE IF NOT EXISTS kv (collection TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (collection, key))');
		if (jsonFiles) this._autoMigrate(Array.isArray(jsonFiles) ? jsonFiles : [jsonFiles]);
	}

	private _journalMode(): string {
		try {
			const row = this.db.prepare('PRAGMA journal_mode').get() as { journal_mode?: string } | undefined;
			return String(row?.journal_mode || '').toLowerCase();
		} catch {
			return '';
		}
	}

	private _ensureWal() {
		if (this._journalMode() === 'wal') return;
		try { this.db.exec('PRAGMA journal_mode = WAL'); } catch {}
		const mode = this._journalMode();
		if (mode === 'wal') {
			this._walWarned = false;
			return;
		}
		if (!this._walWarned) {
			this._walWarned = true;
			console.warn(`[sqliteDB] journal_mode=${mode || 'unknown'} (bukan WAL) — proses lain yang membaca bisa mengunci writer`);
		}
	}

	private _autoMigrate(jsonFiles: string[]) {
		const count = this.db.prepare('SELECT COUNT(*) AS c FROM kv').get() as { c: number };
		if (count.c > 0) return;
		let raw: string;
		let jsonFile = jsonFiles.find(f => fs.existsSync(f));
		if (!jsonFile) return;
		try {
			raw = fs.readFileSync(jsonFile, 'utf8');
		} catch {
			return;
		}
		let data: unknown;
		try {
			data = JSON.parse(raw);
		} catch {
			return;
		}
		if (data && typeof data === 'object') this._writeAll(data as Record<string, unknown>);
	}

	private _writeAll(data: Record<string, unknown>) {
		const entries = Object.entries(data);
		for (let attempt = 0; ; attempt++) {
			try {
				this.db.exec('BEGIN IMMEDIATE');
				try {
					this.db.prepare('DELETE FROM kv').run();
					const ins = this.db.prepare('INSERT OR REPLACE INTO kv (collection, key, value) VALUES (?, ?, ?)');
					for (const [collection, value] of entries) {
						if (value && typeof value === 'object' && !Array.isArray(value)) {
							for (const [k, v] of Object.entries(value)) ins.run(collection, k, JSON.stringify(v));
						} else {
							ins.run(collection, '', JSON.stringify(value));
						}
					}
					this.db.exec('COMMIT');
				} catch (e) {
					try { this.db.exec('ROLLBACK'); } catch {}
					throw e;
				}
				return;
			} catch (e) {
				if (!isLocked(e) || attempt >= WRITE_RETRIES) throw e;
				console.warn(`[sqliteDB] database terkunci, coba lagi ${attempt + 1}/${WRITE_RETRIES}`);
				sleep(RETRY_DELAY);
			}
		}
	}

	async read(): Promise<unknown> {
		const rows = this.db.prepare('SELECT collection, key, value FROM kv').all() as { collection: string; key: string; value: string }[];
		const out: Record<string, Record<string, unknown>> = {};
		for (const row of rows) {
			let value: unknown;
			try {
				value = JSON.parse(row.value);
			} catch {
				value = row.value;
			}
			if (row.key === '') out[row.collection] = value as Record<string, unknown>;
			else (out[row.collection] ||= {})[row.key] = value;
		}
		return out;
	}

	async write(data: unknown): Promise<void> {
		if (this._journalMode() !== 'wal') this._ensureWal();
		if (data && typeof data === 'object') this._writeAll(data as Record<string, unknown>);
	}

	close() {
		this.db.close();
	}
}

export default SQLiteAdapter;
