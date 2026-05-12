import { PrismaClient } from '@prisma/client';
const prisma = new PrismaClient();

async function main() {
  const anomaly = await prisma.anomaly.findUnique({
    where: { id: '647d6623-aeab-4dee-a73b-f24d96ef5e5d' },
    include: { dataset: true }
  });

  if (anomaly) {
    console.log(anomaly.dataset.userId);
  }
}

main()
  .finally(async () => {
    await prisma.$disconnect();
  });
