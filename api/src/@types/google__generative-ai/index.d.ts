/**
 * Minimal type stub for @google/generative-ai.
 * Replace with actual types once the package is installed:
 *   cd backend/api && npm install @google/generative-ai
 */
declare module '@google/generative-ai' {
  export class GoogleGenerativeAI {
    constructor(apiKey: string);
    getGenerativeModel(options: {
      model: string;
      generationConfig?: {
        temperature?: number;
        responseMimeType?: string;
        maxOutputTokens?: number;
      };
    }): GenerativeModel;
  }

  export interface GenerativeModel {
    generateContent(prompt: string): Promise<GenerateContentResult>;
  }

  export interface GenerateContentResult {
    response: {
      text(): string;
    };
  }
}
