import {
  KiipDatabase,
  KiipFragment,
  KiipDocumentInternal,
  Timestamp,
  OnFragment,
  createKiipCallbackSync,
  createKiipPromise
} from '@kiip/core';
import Database, { Options } from 'better-sqlite3';

export type Transaction = null;

interface DatabaseFragment {
  document_id: string;
  timestamp: string;
  table_name: string;
  row: string;
  column: string;
  value: string;
}

export function KiipSQLite(path: string, options: Options = {}): KiipDatabase<Transaction> {
  const db = new Database(path, {
    ...options,
    readonly: false
  });

  db.prepare(
    `CREATE TABLE IF NOT EXISTS fragments (
      document_id TEXT,
      timestamp TEXT,
      table_name TEXT,
      row TEXT,
      column TEXT,
      value TEXT,
      PRIMARY KEY(timestamp, document_id)
    )`
  ).run();
  db.prepare(
    `CREATE TABLE IF NOT EXISTS documents (
      id TEXT PRIMARY KEY,
      name TEXT,
      node_id TEXT
    )`
  ).run();

  return {
    withTransaction(exec) {
      return createKiipPromise(resolve => {
        db.prepare('BEGIN').run();
        return exec(null, val => {
          db.prepare('COMMIT').run();
          return resolve(val);
        });
      });
    },
    addDocument(_tx, document, onResolve) {
      const insertDocumentQuery = db.prepare<KiipDocumentInternal>(
        `INSERT INTO documents (id, name, node_id) VALUES (@id, @name, @nodeId)`
      );
      return createKiipCallbackSync(() => {
        insertDocumentQuery.run(document);
      }, onResolve);
    },
    addFragments(_tx, fragments, onResolve) {
      const insertFragmentQuery = db.prepare<DatabaseFragment>(
        `INSERT INTO fragments (document_id, timestamp, table, row, column, value) VALUES (@document_id, @timestamp, @table, @row, @column, @value)`
      );
      return createKiipCallbackSync(() => {
        fragments.forEach(fragment => {
          insertFragmentQuery.run({
            document_id: fragment.documentId,
            timestamp: fragment.timestamp,
            table_name: fragment.table,
            row: fragment.row,
            column: fragment.column,
            value: JSON.stringify({ value: fragment.value })
          });
        });
      }, onResolve);
    },
    getDocument() {},
    getDocuments() {},
    getFragmentsSince() {},
    onEachFragment() {}
  };
}
