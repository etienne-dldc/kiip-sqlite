import { KiipDatabase, KiipDocument, createKiipCallbackSync, createKiipPromise } from '@kiip/core';
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

interface DatabaseDocument {
  id: string;
  node_id: string;
  meta: string;
}

export function KiipSQLite(
  path: string,
  options: Options = {}
): KiipDatabase<Transaction, unknown> {
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
      node_id TEXT,
      meta TEXT
    )`
  ).run();

  const insertFragmentQuery = db.prepare<DatabaseFragment>(
    `INSERT INTO fragments (document_id, timestamp, table_name, row, column, value)
     VALUES (@document_id, @timestamp, @table_name, @row, @column, @value)
     ON CONFLICT DO NOTHING`
  );

  const beginQuery = db.prepare('BEGIN');

  const commitQuery = db.prepare('COMMIT');

  const insertDocumentQuery = db.prepare<DatabaseDocument>(
    `INSERT INTO documents (id, node_id, meta)
     VALUES (@id, @node_id, @meta)`
  );

  const findDocumentQuery = db.prepare<string>(`SELECT * FROM documents WHERE id = ?`);

  const findDocumentsQuery = db.prepare(`SELECT * FROM documents`);

  const findFragmentSinceQuery = db.prepare<{
    document_id: string;
    timestamp: string;
    ignore_node: string;
  }>(
    `SELECT * FROM fragments WHERE document_id = @document_id AND timestamp > @timestamp AND timestamp NOT LIKE '%' || @ignore_node ORDER BY timestamp`
  );

  const findAllFragmentQuery = db.prepare<{ document_id: string }>(
    `SELECT * FROM fragments WHERE document_id = @document_id ORDER BY timestamp`
  );

  const setMetaQuery = db.prepare<{ document_id: string; meta: string }>(
    `UPDATE documents
     SET meta = @meta
     WHERE id = @document_id LIMIT 1`
  );

  return {
    withTransaction(exec) {
      return createKiipPromise(resolve => {
        beginQuery.run();
        return exec(null, val => {
          commitQuery.run();
          return resolve(val);
        });
      });
    },
    addDocument(_tx, document, onResolve) {
      return createKiipCallbackSync(() => {
        insertDocumentQuery.run({
          id: document.id,
          node_id: document.nodeId,
          meta: serializeValue(document.meta)
        });
      }, onResolve);
    },
    addFragments(_tx, fragments, onResolve) {
      return createKiipCallbackSync(() => {
        fragments.forEach(fragment => {
          insertFragmentQuery.run({
            document_id: fragment.documentId,
            timestamp: fragment.timestamp,
            table_name: fragment.table,
            row: fragment.row,
            column: fragment.column,
            value: serializeValue(fragment.value)
          });
        });
      }, onResolve);
    },
    getDocument(_, documentId, onResolve) {
      return createKiipCallbackSync(() => {
        const doc: DatabaseDocument = findDocumentQuery.get(documentId);
        if (!doc) {
          throw new Error(`Cannot find document ${documentId}`);
        }
        console.log({ doc });
        return {
          id: doc.id,
          nodeId: doc.node_id,
          meta: deserializeValue(doc.meta)
        };
      }, onResolve);
    },
    getDocuments(_, onResolve) {
      return createKiipCallbackSync(() => {
        const docs: Array<DatabaseDocument> = findDocumentsQuery.all();
        console.log({ docs });
        return docs.map(doc => ({
          id: doc.id,
          nodeId: doc.node_id,
          meta: deserializeValue(doc.meta)
        }));
      }, onResolve);
    },
    getFragmentsSince(_, documentId, timestamp, skipNodeId, onResolve) {
      return createKiipCallbackSync(() => {
        const frags: Array<DatabaseFragment> = findFragmentSinceQuery.all({
          document_id: documentId,
          ignore_node: skipNodeId,
          timestamp: timestamp.toString()
        });
        return frags.map(({ column, document_id, row, table_name, timestamp, value }) => ({
          documentId: document_id,
          timestamp,
          table: table_name,
          column,
          row,
          value: deserializeValue(value)
        }));
      }, onResolve);
    },
    onEachFragment(_, documentId, onFragment, onResolve) {
      return createKiipCallbackSync(() => {
        const frags = findAllFragmentQuery.iterate({ document_id: documentId });
        let result = frags.next();
        while (!result.done) {
          const {
            column,
            document_id,
            row,
            table_name,
            timestamp,
            value
          }: DatabaseFragment = result.value;
          onFragment({
            documentId: document_id,
            timestamp,
            table: table_name,
            column,
            row,
            value: deserializeValue(value)
          });
          result = frags.next();
        }
      }, onResolve);
    },
    setMetadata(_, documentId, meta, onResolve) {
      return createKiipCallbackSync(() => {
        setMetaQuery.run({ document_id: documentId, meta: serializeValue(meta) });
      }, onResolve);
    }
  };
}

function serializeValue(value: any): string {
  return JSON.stringify({ value });
}

function deserializeValue(value: string): any {
  return JSON.parse(value).value;
}
