-- AlterTable
ALTER TABLE "anomalies" ADD COLUMN     "ai_suggestion" TEXT;

-- AlterTable
ALTER TABLE "users" ALTER COLUMN "password_hash" DROP DEFAULT;
