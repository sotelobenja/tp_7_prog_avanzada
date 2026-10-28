const { spawn } = require('child_process');

const root = __dirname.replace(/\\scripts?$/, '');
const services = [
  { name: 'proyector', cmd: 'node', args: ['services/proyector.js'] },
  { name: 'notificaciones', cmd: 'node', args: ['services/notificaciones.js'] },
  { name: 'reprogramacion', cmd: 'node', args: ['services/reprogramacion.js'] },
  { name: 'monitor', cmd: 'node', args: ['services/monitor-sla.js'] },
  { name: 'analitica', cmd: 'node', args: ['services/analitica.js'] },
  { name: 'dlq', cmd: 'node', args: ['services/dlq.js'] },
  { name: 'simulator', cmd: 'node', args: ['simulador.js'] },
  { name: 'web', cmd: 'npx', args: ['next', 'dev', '-p', '3000'] }
];

function startService(def) {
  const child = spawn(def.cmd, def.args, {
    cwd: root,
    stdio: 'inherit',
    shell: true
  });

  child.on('exit', (code) => {
    if (code !== 0) {
      console.error(`${def.name} terminó con código ${code}`);
    }
  });

  return child;
}

console.log('Iniciando entorno RutaSur...');
services.forEach((service) => startService(service));
console.log('Todos los servicios fueron lanzados.');
