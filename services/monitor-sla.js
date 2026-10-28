const { createConsumer } = require('../lib/kafka');
const { withCollection } = require('../lib/mongo');

function hoursUntil(dateString) {
  if (!dateString) return Number.POSITIVE_INFINITY;
  const target = new Date(dateString).getTime();
  const now = Date.now();
  return (target - now) / (1000 * 60 * 60);
}

async function ensureAlert(shipmentId, regla, prioridad, descripcion) {
  await withCollection('alertas', async (alertas) => {
    const existing = await alertas.findOne({ shipmentId, regla, resuelta: false });
    if (existing) return;

    await alertas.insertOne({
      shipmentId,
      regla,
      prioridad,
      descripcion,
      createdAt: new Date(),
      resuelta: false
    });
  });
}

async function procesarPromesa(evento) {
  await withCollection('envios', async (envios) => {
    await envios.updateOne(
      { shipmentId: evento.shipmentId },
      {
        $set: {
          promesa: {
            venceEn: evento.payload?.venceEn,
            actualizadoEn: new Date().toISOString()
          }
        }
      },
      { upsert: true }
    );
  });
}

async function evaluarSla(shipmentId, evento) {
  await withCollection('envios', async (envios) => {
    const envio = await envios.findOne({ shipmentId });
    if (!envio) return;

    const promesa = envio.promesa?.venceEn;
    const horas = hoursUntil(promesa);

    if (evento.tipo === 'ENTREGA_FALLIDA') {
      await ensureAlert(shipmentId, 'fallo-dia-limite', 'ALTA', 'Entrega fallida con promesa cercana al vencimiento');
    }

    if (!envio.historial?.some((entry) => entry.tipo === 'INGRESADO_HUB') && horas <= 10 && horas > 0) {
      await ensureAlert(shipmentId, 'sin-ingreso', 'MEDIA', 'Falta ingreso al hub antes del vencimiento');
    }

    if (!envio.historial?.some((entry) => entry.tipo === 'EN_REPARTO') && horas <= 4 && horas > 0) {
      await ensureAlert(shipmentId, 'sin-salida', 'ALTA', 'Falta salida a reparto antes del vencimiento');
    }

    if (envio.estado === 'ENTREGADO') {
      await withCollection('alertas', async (alertas) => {
        await alertas.updateMany(
          { shipmentId, resuelta: false },
          { $set: { resuelta: true, resueltaEn: new Date() } }
        );
      });
    }
  });
}

async function procesarEvento(evento) {
  if (evento.tipo === 'PROMESA_ESTABLECIDA') {
    await procesarPromesa(evento);
    return;
  }

  await evaluarSla(evento.shipmentId, evento);
}

async function start() {
  const consumer = await createConsumer({ groupId: 'monitor-sla', topics: ['envios.estados', 'envios.promesas'] });
  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const value = message.value.toString();
        if (!value) return;
        const evento = JSON.parse(value);
        await procesarEvento(evento);
      } catch (error) {
        console.error('Fallo en monitor SLA:', error);
      }
    }
  });
  console.log('Monitor SLA activo');
}

start().catch((error) => {
  console.error('Fallo al iniciar monitor SLA:', error);
  process.exit(1);
});
