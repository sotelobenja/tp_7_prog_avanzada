const { Kafka } = require('kafkajs');
const crypto = require('crypto');

const kafka = new Kafka({
  clientId: 'rutasur-simulador',
  brokers: [process.env.KAFKA_BROKERS || 'localhost:9092']
});

const producer = kafka.producer();

const CONFIG = {
  enviosPorMinuto: 20,
  factorAceleracion: 30,
  probabilidadFallo: 0.15,
  probabilidadDuplicado: 0.02,
  probabilidadFueraOrden: 0.02,
  probabilidadMalformado: 0.01
};

const HUBS = ['HUB-CONCEPCION', 'HUB-PARANA', 'HUB-COLON'];
const ZONAS = ['ZONA-NORTE', 'ZONA-SUR', 'ZONA-CENTRO'];
const REPARTIDORES = ['REP-01', 'REP-02', 'REP-03', 'REP-04'];
const MOTIVOS_FALLO = ['AUSENTE', 'DIRECCION_INCORRECTA', 'RECHAZADO', 'ZONA_INACCESIBLE'];

async function conectarKafka() {
  await producer.connect();
  console.log('Simulador conectado a Kafka');
}

const esperar = (ms) => new Promise((resolve) => setTimeout(resolve, ms / (CONFIG.factorAceleracion / 30)));

async function simularCicloEnvio() {
  const shipmentId = `ship-${crypto.randomUUID().substring(0, 8)}`;
  const hubId = HUBS[Math.floor(Math.random() * HUBS.length)];
  const zonaId = ZONAS[Math.floor(Math.random() * ZONAS.length)];
  const repartidorId = REPARTIDORES[Math.floor(Math.random() * REPARTIDORES.length)];

  let secuencia = 1;
  const now = new Date();
  const venceEn = new Date(now.getTime() + (24 * 60 * 60 * 1000));

  const eventoCreado = {
    eventId: crypto.randomUUID(),
    shipmentId,
    tipo: 'CREADO',
    ocurridoEn: new Date().toISOString(),
    secuencia: secuencia++,
    hubId,
    zonaId,
    repartidorId,
    version: 1,
    payload: { cliente: 'E-commerce Local', destino: 'Calle Falsa 123' }
  };

  const eventoPromesa = {
    eventId: crypto.randomUUID(),
    shipmentId,
    tipo: 'PROMESA_ESTABLECIDA',
    ocurridoEn: new Date().toISOString(),
    secuencia: secuencia++,
    hubId,
    zonaId,
    version: 1,
    payload: { venceEn: venceEn.toISOString() }
  };

  try {
    await producer.send({
      topic: 'envios.promesas',
      messages: [{ key: shipmentId, value: JSON.stringify(eventoPromesa) }]
    });

    await publicarEvento('envios.estados', shipmentId, eventoCreado);
    console.log(`Envío creado: ${shipmentId}`);

    await esperar(2000);

    await publicarEvento('envios.estados', shipmentId, {
      eventId: crypto.randomUUID(),
      shipmentId,
      tipo: 'INGRESADO_HUB',
      ocurridoEn: new Date().toISOString(),
      secuencia: secuencia++,
      hubId,
      zonaId,
      version: 1,
      payload: {}
    });

    await esperar(3000);

    await publicarEvento('envios.estados', shipmentId, {
      eventId: crypto.randomUUID(),
      shipmentId,
      tipo: 'CLASIFICADO',
      ocurridoEn: new Date().toISOString(),
      secuencia: secuencia++,
      hubId,
      zonaId,
      repartidorId,
      version: 1,
      payload: {}
    });

    await esperar(3000);

    await publicarEvento('envios.estados', shipmentId, {
      eventId: crypto.randomUUID(),
      shipmentId,
      tipo: 'EN_REPARTO',
      ocurridoEn: new Date().toISOString(),
      secuencia: secuencia++,
      hubId,
      zonaId,
      repartidorId,
      version: 1,
      payload: {}
    });

    await esperar(4000);

    const esFallo = Math.random() < CONFIG.probabilidadFallo;
    if (esFallo) {
      const motivo = MOTIVOS_FALLO[Math.floor(Math.random() * MOTIVOS_FALLO.length)];

      let eventoFallo;
      if (Math.random() < CONFIG.probabilidadMalformado) {
        eventoFallo = { malformado: true, shipmentId };
      } else {
        eventoFallo = {
          eventId: crypto.randomUUID(),
          shipmentId,
          tipo: 'ENTREGA_FALLIDA',
          ocurridoEn: new Date().toISOString(),
          secuencia: secuencia++,
          hubId,
          zonaId,
          repartidorId,
          version: 1,
          payload: { motivo }
        };
      }

      await publicarEvento('envios.estados', shipmentId, eventoFallo);
      console.log(`Envío con entrega fallida (${motivo}): ${shipmentId}`);
    } else {
      await publicarEvento('envios.estados', shipmentId, {
        eventId: crypto.randomUUID(),
        shipmentId,
        tipo: 'ENTREGADO',
        ocurridoEn: new Date().toISOString(),
        secuencia: secuencia++,
        hubId,
        zonaId,
        repartidorId,
        version: 1,
        payload: { recibidoPor: 'Titular' }
      });
      console.log(`Envío entregado con éxito: ${shipmentId}`);
    }
  } catch (error) {
    console.error(`Error simulando envío ${shipmentId}:`, error);
  }
}

async function publicarEvento(topic, key, evento) {
  const mensajes = [{ key, value: JSON.stringify(evento) }];

  if (Math.random() < CONFIG.probabilidadDuplicado && topic === 'envios.estados') {
    mensajes.push({ key, value: JSON.stringify(evento) });
    console.log(`[TEST] Duplicando evento artificialmente para ${key}`);
  }

  if (Math.random() < CONFIG.probabilidadFueraOrden && evento.secuencia > 2) {
    evento.secuencia = evento.secuencia - 1;
    mensajes[0].value = JSON.stringify(evento);
    console.log(`[TEST] Enviando evento fuera de orden para ${key}`);
  }

  await producer.send({ topic, messages: mensajes });
}

async function iniciarSimulador() {
  await conectarKafka();
  const intervaloMs = (60 / CONFIG.enviosPorMinuto) * 1000;
  console.log(`Simulador corriendo con intervalo ${(intervaloMs / 1000).toFixed(1)}s`);

  setInterval(() => {
    simularCicloEnvio();
  }, intervaloMs);
}

iniciarSimulador().catch(console.error);