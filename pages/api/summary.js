const { withCollection } = require('../../lib/mongo');

export default async function handler(req, res) {
  try {
    const [envios, alertas, metricas] = await Promise.all([
      withCollection('envios', async (collection) => collection.countDocuments()),
      withCollection('alertas', async (collection) => collection.countDocuments({ resuelta: false })),
      withCollection('metricas_diarias', async (collection) => collection.find({}).limit(20).toArray())
    ]);

    return res.status(200).json({
      totalEnvios: envios,
      alertasAbiertas: alertas,
      metricas
    });
  } catch (error) {
    console.error('Error armado del resumen:', error);
    return res.status(500).json({ error: 'No se pudo obtener el resumen.' });
  }
}
