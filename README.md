# RutaSur TP7

Proyecto de caso práctico de logística orientada a eventos con Kafka, MongoDB y Next.js.

## Requisitos

- Docker Desktop
- Node.js 18+
- npm

## Inicio rápido

1. Instalar dependencias:
   npm install
2. Levantar infraestructura:
   npm run infra
3. Verificar tópicos Kafka:
   docker compose logs kafka-init
4. Iniciar todo el sistema:
   npm run start:all
5. Ejecutar escenarios de demo (opcional):
   npm run demo:feliz
   npm run demo:ausente
   npm run demo:demorado
   npm run demo:tercer-fallo
6. Abrir la interfaz:
   http://localhost:3000

## Parar todo

```bash
npm run stop:all
npm run infra:down
```

## Scripts principales

```bash
npm run simulator
npm run proyector
npm run notificaciones
npm run reprogramacion
npm run monitor
npm run analitica
npm run dlq
npm run web
npm run start:all
npm run stop:all
```

## Estado de la implementación

- Simulador de eventos de envío
- Proyección a MongoDB
- Validación de eventos y DLQ
- Reprogramación por motivo de fallo
- Monitor de SLA con alertas
- Analítica por zona y causa de error
- Frontend de torre de control con métricas y alertas
- Escenarios de demo: feliz, ausente, demorado y tercer-fallo

## Descripción funcional resumida

RutaSur simula un operador logístico con eventos de envío publicándose en Kafka. El proyector reconstruye el estado de cada envío en MongoDB, el monitor de SLA detecta riesgos, las notificaciones y la reprogramación reaccionan ante fallos, y la analítica consolida los indicadores del negocio para la toma de decisiones.
