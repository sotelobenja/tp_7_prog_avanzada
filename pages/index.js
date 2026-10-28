import { useEffect, useState } from 'react';

export default function Home() {
  const [envios, setEnvios] = useState([]);
  const [alertas, setAlertas] = useState([]);
  const [summary, setSummary] = useState({ totalEnvios: 0, alertasAbiertas: 0, metricas: [] });

  async function cargar() {
    try {
      const [enviosResponse, summaryResponse] = await Promise.all([
        fetch('/api/envios'),
        fetch('/api/summary')
      ]);

      const enviosData = await enviosResponse.json();
      const summaryData = await summaryResponse.json();
      setEnvios(enviosData.envios || []);
      setAlertas(enviosData.alertas || []);
      setSummary(summaryData || { totalEnvios: 0, alertasAbiertas: 0, metricas: [] });
    } catch (error) {
      console.error('Error cargando el panel', error);
    }
  }

  useEffect(() => {
    cargar();
    const interval = setInterval(cargar, 5000);
    return () => clearInterval(interval);
  }, []);

  return (
    <main style={{ padding: 24, fontFamily: 'Arial, sans-serif', background: '#f6f8fa', minHeight: '100vh' }}>
      <h1>RutaSur - Torre de control</h1>
      <p>Seguimiento en vivo de envíos y alertas de SLA.</p>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 16, marginTop: 24 }}>
        <StatCard title="Envíos" value={summary.totalEnvios} />
        <StatCard title="Alertas abiertas" value={summary.alertasAbiertas} />
        <StatCard title="Eventos activos" value={envios.length} />
        <StatCard title="Zonas" value={new Set(envios.map((envio) => envio.zonaId)).size || 0} />
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr', gap: 24, marginTop: 24 }}>
        <section style={{ background: '#fff', borderRadius: 12, padding: 16, border: '1px solid #d0d7de' }}>
          <h2>Últimos envíos</h2>
          {envios.length === 0 ? (
            <p>No hay envíos cargados todavía.</p>
          ) : (
            <div style={{ display: 'grid', gap: 12 }}>
              {envios.map((envio) => (
                <div key={envio.shipmentId} style={{ border: '1px solid #d0d7de', borderRadius: 10, padding: 12 }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <strong>{envio.shipmentId}</strong>
                    <span style={{ background: '#e0f2fe', color: '#075985', padding: '4px 8px', borderRadius: 999 }}>{envio.estado}</span>
                  </div>
                  <div>Hub: {envio.hubId}</div>
                  <div>Zona: {envio.zonaId}</div>
                  <div>Última secuencia: {envio.ultimaSecuencia || 0}</div>
                </div>
              ))}
            </div>
          )}
        </section>

        <section style={{ background: '#fff', borderRadius: 12, padding: 16, border: '1px solid #d0d7de' }}>
          <h2>Alertas</h2>
          {alertas.length === 0 ? (
            <p>Sin alertas abiertas.</p>
          ) : (
            <div style={{ display: 'grid', gap: 10 }}>
              {alertas.map((alerta) => (
                <div key={`${alerta.shipmentId}-${alerta.regla}`} style={{ background: '#fff7ed', border: '1px solid #fdba74', borderRadius: 8, padding: 10 }}>
                  <strong>{alerta.regla}</strong>
                  <div>{alerta.shipmentId}</div>
                  <div>Prioridad: {alerta.prioridad || 'MEDIA'}</div>
                </div>
              ))}
            </div>
          )}
        </section>
      </div>
    </main>
  );
}

function StatCard({ title, value }) {
  return (
    <div style={{ background: '#fff', borderRadius: 12, padding: 16, border: '1px solid #d0d7de' }}>
      <div style={{ color: '#6b7280', fontSize: 12 }}>{title}</div>
      <div style={{ fontSize: 28, fontWeight: 700, marginTop: 8 }}>{value}</div>
    </div>
  );
}
