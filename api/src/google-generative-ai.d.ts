/**
 * Minimal type stub for @google/generative-ai.
 * This file allows TypeScript to compile without the package installed.
 *
 * Install the real package when deploying:
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
