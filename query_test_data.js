import { PrismaClient } from '@prisma/client';
const prisma = new PrismaClient();

async function main() {
  const anomaly = await prisma.anomaly.findFirst({
    include: {
      dataset: true
    }
  });

  if (anomaly) {
    console.log(JSON.stringify({
      anomalyId: anomaly.id,
      datasetId: anomaly.datasetId,
      datasetName: anomaly.dataset.name
    }, null, 2));
  } else {
    console.log("No anomaly found in DB.");
  }
}

main()
  .catch(e => {
    console.error(e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
