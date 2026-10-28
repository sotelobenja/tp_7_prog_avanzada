const { createProducer } = require('../lib/kafka');

async function sendScenario(type) {
  const producer = await createProducer();
  const shipmentId = `demo-${Date.now()}`;

  const base = {
    eventId: `${shipmentId}-${Date.now()}`,
    shipmentId,
    hubId: 'HUB-CONCEPCION',
    zonaId: 'ZONA-NORTE',
    repartidorId: 'REP-01',
    version: 1,
    payload: {}
  };

  const eventos = [
    { ...base, tipo: 'CREADO', secuencia: 1, ocurridoEn: new Date().toISOString(), payload: { cliente: 'Demo', destino: 'Calle Demo 123' } },
    { ...base, eventId: `${shipmentId}-2`, tipo: 'INGRESADO_HUB', secuencia: 2, ocurridoEn: new Date().toISOString() },
    { ...base, eventId: `${shipmentId}-3`, tipo: 'CLASIFICADO', secuencia: 3, ocurridoEn: new Date().toISOString() },
    { ...base, eventId: `${shipmentId}-4`, tipo: 'EN_REPARTO', secuencia: 4, ocurridoEn: new Date().toISOString() }
  ];

  if (type === 'feliz') {
    eventos.push({ ...base, eventId: `${shipmentId}-5`, tipo: 'ENTREGADO', secuencia: 5, ocurridoEn: new Date().toISOString(), payload: { recibidoPor: 'Titular' } });
  }

  if (type === 'ausente') {
    eventos.push({ ...base, eventId: `${shipmentId}-5`, tipo: 'ENTREGA_FALLIDA', secuencia: 5, ocurridoEn: new Date().toISOString(), payload: { motivo: 'AUSENTE' } });
  }

  if (type === 'demorado') {
    eventos.push({ ...base, eventId: `${shipmentId}-5`, tipo: 'ENTREGA_FALLIDA', secuencia: 5, ocurridoEn: new Date().toISOString(), payload: { motivo: 'ZONA_INACCESIBLE' } });
  }

  if (type === 'tercer-fallo') {
    eventos.push({ ...base, eventId: `${shipmentId}-5`, tipo: 'ENTREGA_FALLIDA', secuencia: 5, ocurridoEn: new Date().toISOString(), payload: { motivo: 'RECHAZADO' } });
  }

  for (const evento of eventos) {
    await producer.send({
      topic: 'envios.estados',
      messages: [{ key: shipmentId, value: JSON.stringify(evento) }]
    });
    console.log(`Escenario ${type}: enviado ${evento.tipo} para ${shipmentId}`);
  }

  await producer.disconnect();
}

async function main() {
  const scenario = process.argv[2] || 'feliz';
  await sendScenario(scenario);
}

main().catch((error) => {
  console.error('Fallo enviando escenario:', error);
  process.exit(1);
});
