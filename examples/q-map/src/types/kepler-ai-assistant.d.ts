declare module '@kepler.gl/ai-assistant' {
  import type {MessageModel} from '@openassistant/core';

  export const aiAssistantReducer: any;
  export const setMapBoundary: any;
  export function updateAiAssistantConfig(config: {
    isReady?: boolean;
    baseUrl?: string;
    apiKey?: string;
    temperature?: number;
    topP?: number;
    mapboxToken?: string;
  }): any;
  export function updateAiAssistantMessages(messages: MessageModel[]): any;
  export const AiAssistantComponent: any;
}

declare module '@kepler.gl/ai-assistant/tools/tools' {
  export function setupLLMTools(args: any): Record<string, any>;
}
