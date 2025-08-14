export interface Turn {
  turn_id: string;
  run_id: string;
  agent_info: {
    agent_id: string;
    profile_logical_name?: string;
    assigned_role_name?: string;
  };
  status: 'idle' | 'running' | 'completed_success' | 'completed_error' | 'cancelled' | 'interrupted';
  start_time: string;
  end_time?: string | null;
  inputs?: {
    prompt?: string;
    files?: Array<{
      data: string;
      name: string;
      mimeType: string;
    }>;
  } | null;
  llm_interaction?: {
    status?: 'running' | 'completed' | 'error';
    final_response?: {
      content: string;
    };
    attempts?: Array<{ stream_id: string }>;
  } | null;
  tool_interactions?: Array<ToolInteraction> | null;
}

export interface ToolInteraction {
  tool_call_id: string;
  status: 'running' | 'completed_success' | 'completed_error';
  tool_name: string;
  call: {
    arguments: Record<string, unknown>;
  } | null;
  input_params?: Record<string, unknown>;
  result: {
    content: unknown;
    is_error: boolean;
  } | null;
}
