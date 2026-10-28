const { createConsumer } = require('../lib/kafka');
const { withCollection } = require('../lib/mongo');

const reglas = {
  AUSENTE: { accion: 'reprogramar', dias: 1, franja: 'NOCHE' },
  DIRECCION_INCORRECTA: { accion: 'retener', motivo: 'corrigir_direccion' },
  RECHAZADO: { accion: 'devolver', destino: 'comercio' },
  ZONA_INACCESIBLE: { accion: 'reprogramar', dias: 1, franja: 'MANANA' }
};

async function procesarReprogramacion(evento) {
  if (evento.tipo !== 'ENTREGA_FALLIDA') return;

  const motivo = evento.payload?.motivo || 'DESCONOCIDO';
  const regla = reglas[motivo] || { accion: 'devolver', destino: 'comercio' };

  await withCollection('reprogramaciones', async (reprogramaciones) => {
    const existing = await reprogramaciones.findOne({
      shipmentId: evento.shipmentId,
      motivo,
      resuelta: false
    });

    if (existing) return;

    await reprogramaciones.insertOne({
      shipmentId: evento.shipmentId,
      eventId: evento.eventId,
      motivo,
      regla,
      createdAt: new Date(),
      payload: evento.payload || {},
      resuelta: false
    });
  });
}

async function start() {
  const consumer = await createConsumer({ groupId: 'reprogramacion', topics: ['envios.estados'] });
  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const evento = JSON.parse(message.value.toString());
        if (evento.tipo === 'ENTREGA_FALLIDA') {
          await procesarReprogramacion(evento);
          console.log(`Reprogramación registrada para ${evento.shipmentId}`);
        }
      } catch (error) {
        console.error('Fallo en reprogramación:', error);
      }
    }
  });
  console.log('Servicio reprogramación activo');
}

start().catch((error) => {
  console.error('Fallo al iniciar reprogramación:', error);
  process.exit(1);
});
