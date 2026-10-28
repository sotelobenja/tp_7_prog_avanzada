const { execSync } = require('child_process');

const commands = [
  "taskkill /F /IM node.exe /T",
  "taskkill /F /IM next.exe /T"
];

for (const cmd of commands) {
  try {
    execSync(cmd, { stdio: 'ignore' });
  } catch (error) {
    // Ignora si no hay procesos activos
  }
}

console.log('Procesos de la demo RutaSur detenidos.');
