const { createConsumer, createProducer } = require('../lib/kafka');
const { withCollection } = require('../lib/mongo');
const { validateEvent } = require('../lib/validators');

async function sendToDlq(event, reason) {
  const producer = await createProducer();
  await producer.send({
    topic: 'envios.estados.dlq',
    messages: [{
      key: event.shipmentId,
      value: JSON.stringify({
        ...event,
        motivo: reason,
        recibidoEn: new Date().toISOString()
      })
    }]
  });
  await producer.disconnect();
}

async function markProcessed(eventId, consumerName) {
  await withCollection('eventos_procesados', async (collection) => {
    await collection.updateOne(
      { eventId, consumerName },
      { $set: { processedAt: new Date() } },
      { upsert: true }
    );
  });
}

async function alreadyProcessed(eventId, consumerName) {
  return withCollection('eventos_procesados', async (collection) => {
    const found = await collection.findOne({ eventId, consumerName });
    return !!found;
  });
}

async function upsertShipment(event) {
  await withCollection('envios', async (envios) => {
    const current = await envios.findOne({ shipmentId: event.shipmentId });

    if (current && (event.secuencia <= (current.ultimaSecuencia || 0))) {
      return;
    }

    const historial = current?.historial || [];
    const nuevoItem = {
      eventId: event.eventId,
      tipo: event.tipo,
      secuencia: event.secuencia,
      ocurridoEn: event.ocurridoEn,
      payload: event.payload || {}
    };

    const base = {
      shipmentId: event.shipmentId,
      estado: event.tipo,
      hubId: event.hubId || current?.hubId,
      zonaId: event.zonaId || current?.zonaId,
      repartidorId: event.repartidorId || current?.repartidorId,
      ultimaSecuencia: event.secuencia,
      ultimaActualizacion: event.ocurridoEn,
      ultimoEstado: event.tipo,
      historial: [...historial, nuevoItem]
    };

    if (!current) {
      await envios.insertOne({
        ...base,
        promesa: null,
        intentos: 0,
        createdAt: new Date()
      });
      return;
    }

    await envios.updateOne(
      { shipmentId: event.shipmentId },
      {
        $set: {
          ...base,
          historial: [...historial, nuevoItem]
        }
      }
    );
  });
}

async function start() {
  const consumer = await createConsumer({ groupId: 'proyector', topics: ['envios.estados'] });
  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const evento = JSON.parse(message.value.toString());
        const validation = validateEvent(evento);

        if (!validation.valid) {
          await sendToDlq(evento || {}, validation.reason);
          console.log('Evento inválido enviado a DLQ:', validation.reason, evento);
          return;
        }

        if (await alreadyProcessed(evento.eventId, 'proyector')) {
          return;
        }

        await upsertShipment(evento);
        await markProcessed(evento.eventId, 'proyector');
      } catch (error) {
        console.error('Error procesando evento:', error);
      }
    }
  });
  console.log('Servicio proyector activo');
}

start().catch((error) => {
  console.error('Fallo al iniciar proyector:', error);
  process.exit(1);
});
