const { createConsumer } = require('../lib/kafka');
const { withCollection } = require('../lib/mongo');

function fechaKey(fechaIso) {
  return new Date(fechaIso).toISOString().slice(0, 10);
}

async function incrementarMetricas(evento) {
  const zonaId = evento.zonaId || 'DESCONOCIDA';
  const fecha = fechaKey(evento.ocurridoEn || new Date().toISOString());

  await withCollection('metricas_diarias', async (metricas) => {
    if (evento.tipo === 'ENTREGA_FALLIDA') {
      const motivo = evento.payload?.motivo || 'DESCONOCIDO';
      const filtro = { fecha, zonaId, tipo: 'fallos', motivo };
      const doc = await metricas.findOne(filtro);
      if (doc) {
        await metricas.updateOne(filtro, { $inc: { total: 1 } });
      } else {
        await metricas.insertOne({ ...filtro, total: 1 });
      }
    }

    if (evento.tipo === 'ENTREGADO') {
      const filtro = { fecha, zonaId, tipo: 'entregas' };
      const doc = await metricas.findOne(filtro);
      if (doc) {
        await metricas.updateOne(filtro, { $inc: { total: 1 } });
      } else {
        await metricas.insertOne({ ...filtro, total: 1 });
      }
    }

    if (evento.tipo === 'INGRESADO_HUB') {
      const filtro = { fecha, zonaId, tipo: 'ingresos' };
      const doc = await metricas.findOne(filtro);
      if (doc) {
        await metricas.updateOne(filtro, { $inc: { total: 1 } });
      } else {
        await metricas.insertOne({ ...filtro, total: 1 });
      }
    }
  });
}

async function start() {
  const consumer = await createConsumer({ groupId: 'analitica', topics: ['envios.estados'] });
  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const evento = JSON.parse(message.value.toString());
        if (['ENTREGA_FALLIDA', 'ENTREGADO', 'INGRESADO_HUB'].includes(evento.tipo)) {
          await incrementarMetricas(evento);
          console.log(`Métrica actualizada para ${evento.shipmentId}`);
        }
      } catch (error) {
        console.error('Fallo en analítica:', error);
      }
    }
  });
  console.log('Servicio analítica activo');
}

start().catch((error) => {
  console.error('Fallo al iniciar analítica:', error);
  process.exit(1);
});
