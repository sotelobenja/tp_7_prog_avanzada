const { withCollection } = require('../../lib/mongo');

export default async function handler(req, res) {
  try {
    const [envios, alertas] = await Promise.all([
      withCollection('envios', async (collection) => {
        return collection.find({}).sort({ ultimaActualizacion: -1 }).limit(20).toArray();
      }),
      withCollection('alertas', async (collection) => {
        return collection.find({ resuelta: false }).sort({ createdAt: -1 }).limit(10).toArray();
      })
    ]);

    return res.status(200).json({ envios, alertas });
  } catch (error) {
    console.error('Error leyendo envíos:', error);
    return res.status(500).json({ error: 'No se pudieron consultar los envíos.' });
  }
}
