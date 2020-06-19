import { Kiip } from '@kiip/core';
import { KiipSQLite } from '../src';
import * as path from 'path';

const database = KiipSQLite(path.resolve(__dirname, '..', './example/db.sql'));

const kiip = Kiip(database);

console.log(kiip);
