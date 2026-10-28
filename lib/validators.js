const VALID_EVENT_TYPES = new Set([
  'CREADO',
  'INGRESADO_HUB',
  'CLASIFICADO',
  'EN_REPARTO',
  'ENTREGADO',
  'ENTREGA_FALLIDA',
  'PROMESA_ESTABLECIDA'
]);

const VALID_FAILURE_REASONS = new Set([
  'AUSENTE',
  'DIRECCION_INCORRECTA',
  'RECHAZADO',
  'ZONA_INACCESIBLE'
]);

function validateEvent(event) {
  if (!event || typeof event !== 'object') {
    return { valid: false, reason: 'Evento no es un objeto' };
  }

  const required = ['eventId', 'shipmentId', 'tipo', 'ocurridoEn', 'secuencia'];
  for (const field of required) {
    if (!Object.prototype.hasOwnProperty.call(event, field)) {
      return { valid: false, reason: `Falta campo obligatorio: ${field}` };
    }
  }

  if (!VALID_EVENT_TYPES.has(event.tipo)) {
    return { valid: false, reason: `Tipo no soportado: ${event.tipo}` };
  }

  if (event.tipo === 'ENTREGA_FALLIDA') {
    const motivo = event.payload && event.payload.motivo;
    if (!motivo || !VALID_FAILURE_REASONS.has(motivo)) {
      return { valid: false, reason: 'ENTREGA_FALLIDA sin motivo válido' };
    }
  }

  return { valid: true };
}

module.exports = {
  VALID_EVENT_TYPES,
  VALID_FAILURE_REASONS,
  validateEvent
};
