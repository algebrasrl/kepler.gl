import {
  QMAP_AI_ADD_MESSAGE,
  QMAP_AI_CLEAR_MESSAGES,
  QMAP_AI_CLOSE_PANEL,
  QMAP_AI_SET_ERROR,
  QMAP_AI_SET_LOADING,
  QMAP_AI_TOGGLE_PANEL
} from './actions';

type QMapAiMessage = {
  id: string;
  role: 'user' | 'assistant' | 'system';
  content: string;
  createdAt: string;
};

type QMapAiState = {
  isOpen: boolean;
  messages: QMapAiMessage[];
  loading: boolean;
  error: string | null;
};

const initialState: QMapAiState = {
  isOpen: false,
  messages: [],
  loading: false,
  error: null
};

export default function qMapAiReducer(state = initialState, action: any): QMapAiState {
  switch (action.type) {
    case QMAP_AI_TOGGLE_PANEL:
      return {...state, isOpen: !state.isOpen};
    case QMAP_AI_CLOSE_PANEL:
      return {...state, isOpen: false};
    case QMAP_AI_ADD_MESSAGE:
      return {...state, messages: [...state.messages, action.payload]};
    case QMAP_AI_SET_LOADING:
      return {...state, loading: Boolean(action.payload)};
    case QMAP_AI_SET_ERROR:
      return {...state, error: action.payload || null};
    case QMAP_AI_CLEAR_MESSAGES:
      return {...state, messages: [], error: null};
    default:
      return state;
  }
}
