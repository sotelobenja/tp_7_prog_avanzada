const { createConsumer, createProducer } = require('../lib/kafka');
const { withCollection } = require('../lib/mongo');

async function procesarNotificacion(evento) {
  if (evento.tipo !== 'ENTREGA_FALLIDA') return;

  const motivo = evento.payload?.motivo || 'DESCONOCIDO';
  const payload = {
    shipmentId: evento.shipmentId,
    eventId: evento.eventId,
    motivo,
    tipo: 'AVISO_ENTREGA_FALLIDA',
    createdAt: new Date().toISOString(),
    payload: evento.payload || {}
  };

  await withCollection('notificaciones', async (notificaciones) => {
    await notificaciones.insertOne(payload);
  });

  const producer = await createProducer();
  await producer.send({
    topic: 'notificaciones.enviadas',
    messages: [{
      key: evento.shipmentId,
      value: JSON.stringify({
        kind: 'NOTIFICACION',
        shipmentId: evento.shipmentId,
        eventId: evento.eventId,
        motivo,
        createdAt: new Date().toISOString()
      })
    }]
  });
  await producer.disconnect();
}

async function start() {
  const consumer = await createConsumer({ groupId: 'notificaciones', topics: ['envios.estados'] });
  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const evento = JSON.parse(message.value.toString());
        if (evento.tipo === 'ENTREGA_FALLIDA') {
          await procesarNotificacion(evento);
          console.log(`Notificación registrada para ${evento.shipmentId}`);
        }
      } catch (error) {
        console.error('Fallo en notificaciones:', error);
      }
    }
  });
  console.log('Servicio notificaciones activo');
}

start().catch((error) => {
  console.error('Fallo al iniciar notificaciones:', error);
  process.exit(1);
});
