// Writes a package.json into each build output directory so Node interprets
// the emitted .js files with the correct module system regardless of the
// root package's "type" field.
const fs = require('fs');
const path = require('path');

const targets = [
  { dir: 'dist/cjs', type: 'commonjs' },
  { dir: 'dist/esm', type: 'module' }
];

for (const { dir, type } of targets) {
  const file = path.join(__dirname, '..', dir, 'package.json');
  fs.writeFileSync(file, JSON.stringify({ type }, null, 2) + '\n');
  console.log(`wrote ${dir}/package.json -> { "type": "${type}" }`);
}
