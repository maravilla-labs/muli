import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const src = path.resolve(__dirname, '..', '..', '..', 'proto');
const dst = path.resolve(__dirname, '..', 'proto');

if (!fs.existsSync(src)) {
  throw new Error(`Proto source directory not found: ${src}`);
}

fs.rmSync(dst, { recursive: true, force: true });
fs.cpSync(src, dst, { recursive: true });
