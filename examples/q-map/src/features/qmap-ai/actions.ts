export const QMAP_AI_TOGGLE_PANEL = 'QMAP_AI_TOGGLE_PANEL';
export const QMAP_AI_CLOSE_PANEL = 'QMAP_AI_CLOSE_PANEL';
export const QMAP_AI_ADD_MESSAGE = 'QMAP_AI_ADD_MESSAGE';
export const QMAP_AI_SET_LOADING = 'QMAP_AI_SET_LOADING';
export const QMAP_AI_SET_ERROR = 'QMAP_AI_SET_ERROR';
export const QMAP_AI_CLEAR_MESSAGES = 'QMAP_AI_CLEAR_MESSAGES';

export function toggleQMapAiPanel() {
  return {type: QMAP_AI_TOGGLE_PANEL};
}

export function closeQMapAiPanel() {
  return {type: QMAP_AI_CLOSE_PANEL};
}

export function addQMapAiMessage(message: {
  id: string;
  role: 'user' | 'assistant' | 'system';
  content: string;
  createdAt: string;
}) {
  return {type: QMAP_AI_ADD_MESSAGE, payload: message};
}

export function setQMapAiLoading(loading: boolean) {
  return {type: QMAP_AI_SET_LOADING, payload: loading};
}

export function setQMapAiError(error: string | null) {
  return {type: QMAP_AI_SET_ERROR, payload: error};
}

export function clearQMapAiMessages() {
  return {type: QMAP_AI_CLEAR_MESSAGES};
}
