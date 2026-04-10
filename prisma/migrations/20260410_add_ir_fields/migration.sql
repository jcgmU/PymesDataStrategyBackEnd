-- AlterTable
ALTER TABLE "decisions" ADD COLUMN     "correction_ir" JSONB,
ADD COLUMN     "ir_source" TEXT,
ADD COLUMN     "ir_raw_text" TEXT;
