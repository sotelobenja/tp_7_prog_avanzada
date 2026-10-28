const { createConsumer } = require('../lib/kafka');

async function start() {
  const consumer = await createConsumer({ groupId: 'dlq-review', topics: ['envios.estados.dlq'] });
  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const payload = JSON.parse(message.value.toString());
        console.log('🚨 Evento en DLQ:', payload);
      } catch (error) {
        console.error('Error leyendo DLQ:', error);
      }
    }
  });
  console.log('Servicio DLQ activo');
}

start().catch((error) => {
  console.error('Fallo al iniciar DLQ:', error);
  process.exit(1);
});
