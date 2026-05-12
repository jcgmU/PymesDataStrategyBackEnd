import { PrismaClient } from '@prisma/client';

const prisma = new PrismaClient();

async function main() {
  console.log('Fixing stuck datasets...');
  const res = await prisma.dataset.updateMany({
    where: {
      status: 'PROCESSING'
    },
    data: {
      status: 'READY',
      updatedAt: new Date()
    }
  });
  console.log(`Updated ${res.count} datasets from PROCESSING to READY.`);
}

main().catch(console.error).finally(() => prisma.$disconnect());
