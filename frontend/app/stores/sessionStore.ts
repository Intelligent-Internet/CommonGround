import { makeAutoObservable, runInAction, computed } from 'mobx';
import { v4 as uuidv4 } from 'uuid';
import { CSSProperties } from 'react';
import { selectionStore } from './selectionStore'; // Import selectionStore
import { config } from '@/app/config';
import { ProjectService } from '@/lib/api';
import type { Turn as OriginalTurn, ToolInteraction } from '@/app/chat/types/conversation'; // <-- New import

// Define the shape of `llm_interaction` as we expect it, including `actual_usage`.
interface EnrichedLLMInteraction {
    status?: 'running' | 'completed' | 'error';
    final_response?: { content: string };
    attempts?: { stream_id: string }[];
    actual_usage?: {
        prompt_tokens: number;
        completion_tokens: number;
    };
}

// Create a new `Turn` type that replaces `llm_interaction` with our enriched version.
export type Turn = Omit<OriginalTurn, 'llm_interaction'> & {
    llm_interaction?: EnrichedLLMInteraction;
};

// region: ViewModel Types - as per intro_data_view.md
export interface FlowNodeData {
  label: string;
  nodeType: 'principal' | 'agent' | 'tool' | 'dispatch' | 'turn' | 'gather';
  status: 'idle' | 'running' | 'completed_success' | 'completed_error' | 'cancelled';
  content_stream_id?: string | null;
  final_content?: string | null; // <--- New field for final aggregated content
  timestamp?: string;
  originalId?: string;
  
  // ---> New fields <---
  turn_id?: string;
  agent_id?: string;
  depth?: number;

  // DEBUG: For displaying the node's line number (will be removed later)
  debugRowNumber?: number;
  
  // Actual height of the node for precise edge length calculation
  actualHeight?: number;

  // --- New fields (v3.0) ---
  layerMaxContentLevel?: 'XS' | 'S' | 'M' | 'L' | 'XL' | 'XXL';
  
  // For tool nodes, populated when the tool is called
  tool_call_details?: {
    tool_name: string;
    // Arguments are parsed into an object for easy frontend use
    arguments: Record<string, unknown>; 
  } | null;
  
  // For tool nodes, populated after the tool returns a result
  tool_result?: {
    // The result can be of any type, but a serializable object is recommended
    content: unknown; 
    is_error: boolean;
  } | null;

  // For 'turn' nodes, this aggregates multiple tool interactions
  tool_interactions?: Array<ToolInteraction>;
}

export interface FlowNode {
  id: string;
  type: 'custom';
  data: FlowNodeData;
}

export interface FlowEdge {
  id: string;
  source: string;
  target: string;
  animated: boolean;
  style?: CSSProperties; // This field is dynamically added by the frontend and should not be sent by the backend
  edgeType?: 'default' | 'return'; // New semantic field
}

export interface FlowViewModel {
  nodes: Omit<FlowNode, 'position'>[];
  edges: FlowEdge[];
}

export interface EnrichedWorkModule {
  module_id: string;
  name: string;
  description: string;
  status: string;
  updated_at: string;
  assignee_history: Array<{
    dispatch_id: string;
    agent_id: string;
    started_at: string;
    ended_at: string;
    outcome: string;
  }>;
  current_assignee_id?: string;
  live_status_summary?: string;
  latest_deliverables_summary?: string;
  review_info?: {
    trigger: string;
    message: string;
    error_details: string | null;
  } | null;
  is_rework: boolean;
  agent_id?: string;
}

export interface KanbanViewModel {
  view_by_status: {
    pending: EnrichedWorkModule[];
    ongoing: EnrichedWorkModule[];
    pending_review: EnrichedWorkModule[];
    completed: EnrichedWorkModule[];
    deprecated: EnrichedWorkModule[];
  };
  view_by_agent: {
    [agentIdOrUnassigned: string]: EnrichedWorkModule[];
  };
  last_updated: string;
}

export interface TimeBreak {
  breakStart: string;   // ISO timestamp for the start of the idle period
  breakEnd: string;     // ISO timestamp for the end of the idle period
  duration: number;       // Total seconds of this idle period
}

export interface TimelineBlock {
  moduleId: string;
  moduleName: string;
  blockType?: 'task' | 'turn' | 'tool';
  status: string;
  startTime: string; // The actual, unmodified start time
  endTime: string | null;   // The actual, unmodified end time
  agent_id?: string;
}

export interface TimelineViewModel {
  lanes: {
    agentId: string;
    blocks: TimelineBlock[];
  }[];
  overallStartTime: string | null; // Start of the actual time
  overallEndTime: string | null;   // End of the actual time
  timeBreaks: TimeBreak[];
  isLive?: boolean;
}
// endregion

// WebSocket message type definitions
export interface LLMChunk {
  type: 'llm_chunk';
  run_id: string;
  agent_id: string;
  parent_agent_id?: string;
  stream_id: string;
  llm_id?: string;
  chunk_type: 'content' | 'tool_name' | 'tool_args' | 'reasoning';
  is_completion_marker: boolean;
  data: {
    content: string;
  };
}

export interface LLMResponse {
  type: 'llm_response';
  run_id: string;
  agent_id: string;
  parent_agent_id?: string;
  stream_id: string;
  is_completion_marker: boolean;
  data: {
    content?: string;
    tool_calls?: Array<{
      id: string;
      type: 'function';
      function: {
        name: string;
        arguments: string;
      };
    }>;
    reasoning?: string;
  };
}

export interface LLMRequestParams {
  type: 'llm_request_params';
  run_id: string;
  agent_id: string;
  stream_id: string;
  llm_id?: string;
  data: {
    params: Record<string, unknown>; // Sensitive info redacted
  };
}

export interface LLMStreamStarted {
  type: 'llm_stream_started';
  run_id: string;
  agent_id: string;
  parent_agent_id?: string;
  stream_id: string;
  llm_id?: string;
  data: Record<string, unknown>;
}

export interface LLMStreamEnded {
  type: 'llm_stream_ended';
  run_id: string;
  agent_id: string;
  parent_agent_id?: string;
  stream_id: string;
  data: Record<string, unknown>;
}

export interface LLMStreamFailed {
  type: 'llm_stream_failed';
  run_id: string;
  agent_id: string;
  parent_agent_id?: string;
  stream_id: string;
  data: {
    reason: string;
  };
}

export interface StateSync {
  type: 'state_sync';
  run_id: string;
  event_tag: string;
  data: {
    state: Record<string, unknown>;
  };
}

export interface AgentStatus {
  type: 'agent_status';
  run_id?: string;
  agent_id: string;
  parent_agent_id?: string;
  data: {
    status: string;
    details?: Record<string, unknown>;
  };
}

export interface ErrorMessage {
  type: 'error';
  run_id?: string;
  agent_id?: string;
  data: {
    message: string;
  };
}

export interface RunReady {
  type: 'run_ready';
  data: {
    request_id: string;
    run_id: string;
    status: 'success';
  };
}

export interface AvailableToolsetsResponse {
  type: 'available_toolsets_response';
  run_id: null; // This message is not specific to a run
  data: {
    toolsets: Record<string, Array<{
      name: string;
      description: string;
      parameters: Record<string, unknown>; // JSON Schema
    }>>;
  };
}

export interface ToolResult {
  type: 'tool_result';
  run_id?: string;
  agent_id: string;
  parent_agent_id?: string;
  data: {
    tool_call_id: string;
    tool_name: string;
    content: string;
    is_error: boolean;
  };
}

export interface DispatchOperationResult {
  type: 'dispatch_operation_result';
  run_id: string; // Principal's run_id
  source_agent_id: string; // DispatcherNode_ID
  data: {
    dispatch_tool_call_id: string;
    overall_status: 'SUCCESS' | 'PARTIAL_FAILURE' | 'TOTAL_FAILURE';
    message: string;
    attempted_assignments: Array<{
      requested_task_nums: number[];
      requested_profile_logical_name: string;
      launch_outcome: 'SUCCESS' | 'FAILURE_PROFILE_NOT_FOUND' | 'FAILURE_TASK_INVALID' | 'FAILURE_OTHER_PREP_ERROR';
      executing_associate_id: string | null;
      failure_reason: string | null;
    }>;
  };
}

export interface AssociateExecutionStarted {
  type: 'associate_execution_started';
  run_id: string; // Principal's run_id
  source_agent_id: string; // This Associate's executing_associate_id
  data: {
    assigned_task_nums: number[];
    profile_logical_name_used: string;
    profile_instance_id_used: string;
    task_module_description: string;
    start_timestamp: string; // ISO8601_timestamp
    dispatch_tool_call_id_ref?: string;
  };
}

export interface AssociateExecutionEnded {
  type: 'associate_execution_ended';
  run_id: string; // Principal's run_id
  source_agent_id: string; // This Associate's executing_associate_id
  data: {
    assigned_task_nums: number[];
    profile_logical_name_used: string;
    completion_status: 'COMPLETED_SUCCESS' | 'COMPLETED_ERROR' | 'CANCELLED';
    final_summary: string | null;
    error_details: string | null;
    end_timestamp: string; // ISO8601_timestamp
    dispatch_tool_call_id_ref?: string;
  };
}

export interface WorkModuleUpdated {
  type: 'work_module_updated';
  run_id: string;
  session_id: string;
  data: {
    module: {
      module_id: string;
      name: string;
      description: string;
      created_at: string;
      updated_at: string;
      status: string;
      notes_from_principal: string;
      review_info: {
        trigger: string;
        message: string;
        error_details: string | null;
      } | null;
      assignee_history: Array<{
        dispatch_id: string;
        agent_id: string;
        started_at: string;
        ended_at: string;
        outcome: string;
      }>;
      context_archive: Array<{
        dispatch_id: string;
        archived_at: string;
        deliverables: {
          primary_summary: string;
        };
        messages: Array<{
          role: string;
          content: string;
          tool_call_id?: string;
          name?: string;
          tool_calls?: Array<{
            id: string;
            type: string;
            function: {
              name: string;
              arguments: string;
            };
          }>;
        }>;
      }>;
    };
  };
}

export interface ViewModelUpdate {
  type: 'view_model_update';
  data: {
    view_name: 'flow_view' | 'kanban_view' | 'timeline_view';
    model: FlowViewModel | KanbanViewModel | TimelineViewModel;
  };
}

export interface ViewModelUpdateFailed {
  type: 'view_model_update_failed';
  data: {
    view_name: string;
    error: string;
  };
}

export interface TurnsSync {
  type: 'turns_sync';
  data: {
    turns: Turn[];
  };
}

export interface ProjectStructureUpdated {
  type: 'project_structure_updated';
  data: {
    project_id: string;
  };
}

export interface TokenUsageStats {
    total_prompt_tokens: number;
    total_completion_tokens: number;
    max_context_window: number;
    total_successful_calls: number;
    total_failed_calls: number;
}

export interface TokenUsageUpdate {
  type: 'token_usage_update';
  data: TokenUsageStats;
}

export type WebSocketMessage =
  | LLMChunk
  | LLMResponse
  | StateSync
  | AgentStatus
  | ErrorMessage
  | RunReady
  | AvailableToolsetsResponse
  | LLMRequestParams
  | LLMStreamStarted
  | LLMStreamEnded
  | LLMStreamFailed
  | ToolResult
  | DispatchOperationResult
  | AssociateExecutionStarted
  | AssociateExecutionEnded
  | WorkModuleUpdated
  | ViewModelUpdate
  | ViewModelUpdateFailed
  | TurnsSync
  | ProjectStructureUpdated
  | TokenUsageUpdate;

export interface DispatchHistoryEntry {
  dispatch_instance_id: string;
  assigned_task_nums: number[];
  profile_logical_name: string;
  status: 'LAUNCHING' | 'RUNNING' | 'COMPLETED_SUCCESS' | 'COMPLETED_ERROR' | 'CANCELLED';
  task_module_description: string;
  start_timestamp: string | null;
  end_timestamp: string | null;
  final_summary: string | null;
  error_details: string | null;
  dispatch_tool_call_id_ref?: string;
}



class SessionStore {
  sessionId: string | null = null;
  ws: WebSocket | null = null;
  isConnected: boolean = false;
  isConnecting: boolean = false;
  error: string | null = null;
  isResuming: boolean = false; 
  private maxRetryAttempts: number = 3;
  private retryDelayMs: number = 1000; // 1-second delay
  useLLMChunk = true;
  currentlySubscribedRunId: string | null = null;

  // ViewModel/Sync state
  flowStructure: FlowViewModel | null = null;
  kanbanViewModel: KanbanViewModel | null = null;
  timelineViewModel: TimelineViewModel | null = null;
  streamingContent = new Map<string, string>();
  viewErrors = new Map<string, string | null>();
  
  // New: Track if waiting for a new ViewModel
  isWaitingForNewViewModel: boolean = false;

  // For smooth streaming output
  private streamingCharQueue = new Map<string, string[]>();
  private streamingInterval: NodeJS.Timeout | null = null;
  private readonly STREAMING_SPEED_MS = 5; // ms interval for updates
  private readonly CHARS_PER_INTERVAL = 5;  // chars to add per interval

  // --- New properties ---
  turns: Turn[] = [];
  tokenUsageStats: TokenUsageStats | null = null;
  // --- End of new properties ---

  // New: Run creation completion event counter, used to trigger project list refresh
  runCreatedCounter: number = 0;

  private runCreationPromises = new Map<string, { resolve: (runId: string) => void, reject: (reason?: unknown) => void }>();

  // New store properties for API updates
  availableToolsets: AvailableToolsetsResponse['data']['toolsets'] | null = null;
  lastLLMRequestParams: LLMRequestParams['data'] | null = null;
  toolResultsLog: ToolResult[] = [];
  dispatchOperationLogs: DispatchOperationResult[] = [];
  associateExecutionStates: Record<string, AssociateExecutionStarted['data'] | AssociateExecutionEnded['data']> = {};
  // Agent stream states for LLM flow tracking
  agentStreamStates: Record<string, Record<string, 'llm_stream_started' | 'llm_stream_ended' | 'llm_stream_failed'>> = {};
  // Work modules state
  workModules: Record<string, Record<string, WorkModuleUpdated['data']['module']>> = {};

  constructor() {
    makeAutoObservable(this, {
        chatHistoryTurns: computed,
        activityStreamTurns: computed
    });
    this.startStreamingProcessor();
  }

  private startStreamingProcessor() {
    if (this.streamingInterval) {
      clearInterval(this.streamingInterval);
    }
    this.streamingInterval = setInterval(() => {
      this.processStreamingQueue();
    }, this.STREAMING_SPEED_MS);
  }

  private stopStreamingProcessor() {
    if (this.streamingInterval) {
      clearInterval(this.streamingInterval);
      this.streamingInterval = null;
    }
  }

  private processStreamingQueue() {
    if (this.streamingCharQueue.size === 0) {
      return;
    }

    runInAction(() => {
      this.streamingCharQueue.forEach((queue, streamId) => {
        if (queue.length > 0) {
          const charsToAdd = queue.splice(0, this.CHARS_PER_INTERVAL).join('');
          if (charsToAdd) {
            const currentContent = this.streamingContent.get(streamId) || '';
            this.streamingContent.set(streamId, currentContent + charsToAdd);
          }
        }
      });
    });
  }

  get chatHistoryTurns(): Turn[] {
    if (!this.turns) return [];
    // Only include 'User' and 'Partner' turns
    return this.turns.filter(turn => 
        turn.agent_info?.agent_id.includes('User') || 
        turn.agent_info?.agent_id.includes('Partner')
    );
  }

  get activityStreamTurns(): Turn[] {
    if (!this.turns) return [];
    // Exclude 'User' and 'Partner' turns
    return this.turns.filter(turn => 
        !turn.agent_info?.agent_id.includes('User') && 
        !turn.agent_info?.agent_id.includes('Partner')
    );
  }

  // New private helper method
  private _subscribeToAllViews(runId: string) {
    if (this.ws?.readyState === WebSocket.OPEN && runId) {
      console.log(`Subscribing to all views for runId: ${runId}`);
      this.subscribeToView(runId, 'flow_view');
      this.subscribeToView(runId, 'kanban_view');
      this.subscribeToView(runId, 'timeline_view');
      // Update the currently subscribed ID
      this.currentlySubscribedRunId = runId;
    }
  }

  // New private helper method
  private _unsubscribeFromAllViews(runId: string) {
    if (this.ws?.readyState === WebSocket.OPEN && runId) {
      console.log(`Unsubscribing from all views for runId: ${runId}`);
      this.unsubscribeFromView(runId, 'flow_view');
      this.unsubscribeFromView(runId, 'kanban_view');
      this.unsubscribeFromView(runId, 'timeline_view');
    }
  }

  // New: Clear ViewModel and set waiting state
  clearViewModelsAndWait() {
    runInAction(() => {
      this.flowStructure = null;
      this.kanbanViewModel = null;
      this.timelineViewModel = null;
      this.streamingContent.clear();
      this.viewErrors.clear();
      this.isWaitingForNewViewModel = true;
      console.log('🧹 Cleared view models and set waiting state');
    });
  }

  get isSystemRunning() {
    // Based on the new Turn model, we check if there are any running Turns
    return this.ws?.readyState === WebSocket.OPEN && 
           this.turns.some(turn => turn.status === 'running');
  }

  getRunStatus(runId: string): { isRunning: boolean; isStreamStarted: boolean } {
    const runTurns = this.turns.filter(t => t.run_id === runId);
    if (runTurns.length === 0) {
      return { isRunning: false, isStreamStarted: false };
    }

    // If any turn is running, the entire process is considered to be running
    const isRunning = runTurns.some(t => t.status === 'running');

    // If any Turn's LLM interaction is in progress, it is considered that streaming output has started
    const isStreamStarted = runTurns.some(t => 
      t.agent_info.agent_id.includes('Partner') && t.llm_interaction?.status === 'running'
    );

    return { isRunning, isStreamStarted };
  }

  async initializeSessionAndConnect() {
    if (this.isConnected || this.isConnecting) {
        console.log("Session already connected or connecting.");
        return;
    }

    runInAction(() => {
        this.isConnecting = true;
    });

    try {
        const data = await ProjectService.createSession();
        
        runInAction(() => {
            this.sessionId = data.session_id;
        });

        await this.connectWebSocket(data.session_id);

        runInAction(() => {
            this.isConnected = true;
            console.log("WebSocket connection established successfully.");
        });
    } catch (error) {
        console.error('Failed to initialize session and connect WebSocket:', error);
        runInAction(() => {
            this.isConnected = false;
            // Provide a more user-friendly error message
            if (error instanceof Error && error.message === 'Failed to fetch') {
              this.error = 'Unable to connect to server. Please check your network connection and try again.';
            } else {
              this.error = error instanceof Error ? error.message : 'Failed to initialize session';
            }
        });
    } finally {
        runInAction(() => {
            this.isConnecting = false;
        });
    }
  }

  async connectWebSocket(sessionId: string) {
    return new Promise((resolve, reject) => {
      const ws = new WebSocket(`${config.ws.url}${config.ws.endpoint}/${sessionId}`);
      
      ws.onopen = () => {
        this.ws = ws;
        resolve(ws);
      };

      ws.onmessage = (event) => {
        const data = JSON.parse(event.data) as WebSocketMessage;
        this.handleWebSocketMessage(data);
      };

      ws.onerror = (error) => {
        console.error('WebSocket error:', error);
        this.error = 'Connection error. Please try again.';
        reject(error);
      };

      ws.onclose = () => {
        this.ws = null;
      };
    });
  }

  handleWebSocketMessage(data: WebSocketMessage) {
    // console.log('Received message:', data.type, data);
    switch (data.type) {
      case 'turns_sync': {
        runInAction(() => {
            this.turns = data.data.turns;
            this.isResuming = false; // End of resumption
        });
        break;
      }
      case 'llm_chunk': {
        const chunk = data as LLMChunk;
        // Only process content chunks for now, as others are for different purposes
        if (chunk.chunk_type === 'content' && chunk.data.content) {
          if (!this.streamingCharQueue.has(chunk.stream_id)) {
            this.streamingCharQueue.set(chunk.stream_id, []);
          }
          this.streamingCharQueue.get(chunk.stream_id)!.push(...chunk.data.content.split(''));
        }
        break;
      }
      case 'run_ready': {
        const { request_id, run_id } = (data as RunReady).data;
        if (this.runCreationPromises.has(request_id)) {
            this.runCreationPromises.get(request_id)?.resolve(run_id);
            this.runCreationPromises.delete(request_id);
        }
        this.runCreatedCounter += 1;
        break;
      }
      case 'error': {
        const errorData = data as ErrorMessage;
        runInAction(() => {
            this.error = errorData.data.message;
            this.isResuming = false; // Stop resuming on error
        });
        console.error('Error:', errorData.data.message);
        break;
      }
      case 'view_model_update': {
        console.log('View Model Update Received:', data);
        const { view_name, model } = (data as ViewModelUpdate).data;
        this.viewErrors.set(view_name, null);
        
        // Reset waiting state (first ViewModel has arrived)
        if (this.isWaitingForNewViewModel) {
          runInAction(() => {
            this.isWaitingForNewViewModel = false;
            console.log('🎯 First ViewModel received, clearing waiting state');
          });
        }
        
        if (view_name === 'flow_view') {
          // Do not update turns from here anymore
          const newFlow = model as FlowViewModel;
          
          // Create a map of stream IDs from the current (old) flow structure
          // to preserve content of completed streams.
          const oldStreamIds = new Map<string, string>();
          if (this.flowStructure) {
              this.flowStructure.nodes.forEach(node => {
                  if (node.data.content_stream_id) {
                      oldStreamIds.set(node.id, node.data.content_stream_id);
                  }
              });
          }

          // Merge old stream IDs into the new flow structure.
          // If a node in the new structure has a null stream ID, but we have a
          // record of it from the previous structure, we re-apply it.
          // This prevents streamed content from disappearing when a node's status
          // changes from 'running' to 'completed'.
          newFlow.nodes.forEach(newNode => {
              if (!newNode.data.content_stream_id && oldStreamIds.has(newNode.id)) {
                  const preservedStreamId = oldStreamIds.get(newNode.id);
                  if (preservedStreamId) {
                    newNode.data.content_stream_id = preservedStreamId;
                  }
              }
          });
          
          this.flowStructure = newFlow;
        } else if (view_name === 'kanban_view') {
          this.kanbanViewModel = model as KanbanViewModel;
        } else if (view_name === 'timeline_view') {
          this.timelineViewModel = model as TimelineViewModel;
        }
        break;
      }
      case 'available_toolsets_response': {
        this.availableToolsets = (data as AvailableToolsetsResponse).data.toolsets;
        console.log('Available Toolsets Received:', this.availableToolsets);
        break;
      }
      case 'llm_request_params': {
        this.lastLLMRequestParams = (data as LLMRequestParams).data;
        console.log('LLM Request Params Received:', this.lastLLMRequestParams);
        break;
      }
      case 'tool_result': {
        const toolResultData = data as ToolResult;
        this.toolResultsLog.push(toolResultData);
        console.log('Tool Result Received:', toolResultData);
        break;
      }
      case 'dispatch_operation_result': {
        const dispatchData = data as DispatchOperationResult;
        this.dispatchOperationLogs.push(dispatchData);
        console.log('Dispatch Operation Result Received:', dispatchData);
        // This event can be used to give immediate feedback on dispatch attempts.
        break;
      }
      case 'associate_execution_started': {
        const associateStartData = data as AssociateExecutionStarted;
        this.associateExecutionStates[associateStartData.source_agent_id] = associateStartData.data;
        console.log('Associate Execution Started:', associateStartData);
        // Update dispatchHistory or a similar structure if needed for real-time updates
        // For now, relying on state_sync for the full dispatch_history update.
        break;
      }
      case 'associate_execution_ended': {
        const associateEndData = data as AssociateExecutionEnded;
        this.associateExecutionStates[associateEndData.source_agent_id] = associateEndData.data;
        console.log('Associate Execution Ended:', associateEndData);
        // Update dispatchHistory or a similar structure if needed for real-time updates
        break;
      }
      case 'llm_stream_started': {
        const streamStartData = data as LLMStreamStarted;
        // Update agent stream state
        if (!this.agentStreamStates[streamStartData.run_id]) {
          this.agentStreamStates[streamStartData.run_id] = {};
        }
        this.agentStreamStates[streamStartData.run_id][streamStartData.agent_id] = 'llm_stream_started';
        console.log('LLM Stream Started:', streamStartData);
        break;
      }
      case 'llm_stream_ended': {
        const streamEndData = data as LLMStreamEnded;
        // Update agent stream state
        if (!this.agentStreamStates[streamEndData.run_id]) {
          this.agentStreamStates[streamEndData.run_id] = {};
        }
        this.agentStreamStates[streamEndData.run_id][streamEndData.agent_id] = 'llm_stream_ended';
        console.log('LLM Stream Ended:', streamEndData);
        break;
      }
      case 'llm_stream_failed': {
        const streamFailedData = data as LLMStreamFailed;
        // Update agent stream state
        if (!this.agentStreamStates[streamFailedData.run_id]) {
          this.agentStreamStates[streamFailedData.run_id] = {};
        }
        this.agentStreamStates[streamFailedData.run_id][streamFailedData.agent_id] = 'llm_stream_failed';
        console.log('LLM Stream Failed:', streamFailedData);
        break;
      }
      case 'work_module_updated': {
        const workModuleData = data as WorkModuleUpdated;
        if (!this.workModules[workModuleData.run_id]) {
          this.workModules[workModuleData.run_id] = {};
        }
        this.workModules[workModuleData.run_id][workModuleData.data.module.module_id] = workModuleData.data.module;
        console.log('Work Module Updated:', workModuleData);
        break;
      }
      case 'token_usage_update': {
        const tokenData = data as TokenUsageUpdate;
        runInAction(() => {
          this.tokenUsageStats = tokenData.data;
        });
        break;
      }
      case 'project_structure_updated': {
        console.log('Project structure update event received:', data.data);
        // Trigger a global refresh of the project list
        selectionStore.triggerProjectsRefresh();
        break;
      }
      default: {
        console.log('Unknown message type:', data.type);
      }
    }
  }

  async sendMessage(message: string, runId: string, images?: any[]) {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
        console.error("Cannot send message, WebSocket is not open.");
        this.error = "Connection failed. Cannot send message.";
        return;
    }

    // 处理图像数据
    const imageData = images ? await Promise.all(
      images.map(async (img) => {
        // 将文件转换为base64
        const base64 = await this.fileToBase64(img.file);
        return {
          type: 'image',
          data: base64,
          name: img.name,
          mimeType: img.file.type
        };
      })
    ) : [];

    const messagePayload: any = { prompt: message };
    if (imageData.length > 0) {
      messagePayload.images = imageData;
    }

    this.ws.send(JSON.stringify({
        type: 'send_to_run',
        data: {
            run_id: runId,
            message_payload: messagePayload
        }
    }));
  }

  private fileToBase64(file: File): Promise<string> {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => {
        const result = reader.result as string;
        // 移除data:image/xxx;base64,前缀，只保留base64数据
        const base64Data = result.split(',')[1];
        resolve(base64Data);
      };
      reader.onerror = reject;
      reader.readAsDataURL(file);
    });
  }

  async stopExecution(runId: string) {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({
        type: 'stop_run',
        data: {
          run_id: runId
        }
      }));
    }
  }

  private async ensureConnection(): Promise<void> {
    if (this.isConnected && this.ws) {
      return;
    }
    if (this.isConnecting) {
      return new Promise((resolve, reject) => {
        const checkConnection = () => {
          if (this.isConnected) {
            resolve();
          } else if (!this.isConnecting) {
            reject(new Error("Failed to establish connection"));
          } else {
            setTimeout(checkConnection, 100);
          }
        };
        setTimeout(checkConnection, 100);
      });
    }
    let lastError: Error | null = null;
    for (let attempt = 1; attempt <= this.maxRetryAttempts; attempt++) {
      try {
        console.log(`Attempting to reconnect (${attempt}/${this.maxRetryAttempts})...`);
        if (this.sessionId) {
          await this.connectWebSocket(this.sessionId);
          runInAction(() => {
            this.isConnected = true;
            this.error = null;
          });
          console.log("Reconnection successful using existing session");
          return;
        } else {
          await this.initializeSessionAndConnect();
          console.log("Reconnection successful with new session");
          return;
        }
      } catch (error) {
        lastError = error instanceof Error ? error : new Error("Unknown connection error");
        console.warn(`Reconnection attempt ${attempt} failed:`, lastError.message);
        if (attempt < this.maxRetryAttempts) {
          await new Promise(resolve => setTimeout(resolve, this.retryDelayMs * attempt));
        }
      }
    }
    throw new Error(`Failed to reconnect after ${this.maxRetryAttempts} attempts. Last error: ${lastError?.message}`);
  }

  public async createRun(runType: string, projectId: string): Promise<string> {
    await this.ensureConnection();
    const requestId = uuidv4();
    if (this.currentlySubscribedRunId) {
      this._unsubscribeFromAllViews(this.currentlySubscribedRunId);
      this.currentlySubscribedRunId = null;
    }
    this.clearViewModelsAndWait();
    const payload = {
      type: 'start_run',
      data: {
        request_id: requestId,
        run_type: runType,
        project_id: projectId,
      }
    };
    this.ws!.send(JSON.stringify(payload));
    return new Promise((resolve, reject) => {
      this.runCreationPromises.set(requestId, {
        resolve: (runId: string) => {
          this._subscribeToAllViews(runId);
          resolve(runId);
        },
        reject
      });
      setTimeout(() => {
        if (this.runCreationPromises.has(requestId)) {
          this.runCreationPromises.get(requestId)?.reject(new Error("Run creation timed out."));
          this.runCreationPromises.delete(requestId);
        }
      }, 15000);
    });
  }

  async resumeRun(runId: string, projectId: string): Promise<string> {
    await this.ensureConnection();

    // Key: Before initiating a new request, unsubscribe from the old runId and clear the views
    if (this.currentlySubscribedRunId && this.currentlySubscribedRunId !== runId) {
        this._unsubscribeFromAllViews(this.currentlySubscribedRunId);
        this.currentlySubscribedRunId = null;
    }
    // If the runId to be resumed is already the currently subscribed one, no action is needed
    if (this.currentlySubscribedRunId === runId) {
      // Even if it's the same run, we might want to re-trigger the UI states.
      // For now, we just resolve the promise with the existing runId.
      return Promise.resolve(runId);
    }

    // Clear all ViewModels and set waiting state
    this.clearViewModelsAndWait();

    runInAction(() => {
      this.isResuming = true;
      this.error = null; // Clear previous errors
    });

    const requestId = uuidv4();
    const payload = {
        type: 'start_run',
        data: {
            request_id: requestId,
            run_type: 'partner_interaction', // The type for resumption is usually this; the backend can override it from the state file
            resume_from_run_id: runId,       // <--- Key field
            project_id: projectId,
        },
    };
    this.ws!.send(JSON.stringify(payload));

    return new Promise((resolve, reject) => {
        this.runCreationPromises.set(requestId, { 
          resolve: (returnedRunId: string) => {
            this._subscribeToAllViews(returnedRunId);
            resolve(returnedRunId);
          },
          reject 
        });
    });
  }

  async createEmptyRun(projectId: string, filename: string): Promise<string> {
    try {
      // Ensure the connection is established
      await this.ensureConnection();
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : "Failed to establish connection";
      console.error("Cannot create empty run - connection failed:", errorMessage);
      return Promise.reject(new Error(`Cannot create empty run - connection failed: ${errorMessage}`));
    }

    const requestId = uuidv4();
    const payload = {
      type: 'start_run',
      data: {
        request_id: requestId,
        run_type: 'partner_interaction',
        project_id: projectId,
        initial_filename: filename
      }
    };
    this.ws!.send(JSON.stringify(payload));
    
    return new Promise((resolve, reject) => {
      this.runCreationPromises.set(requestId, { resolve, reject });
      setTimeout(() => {
          if (this.runCreationPromises.has(requestId)) {
              this.runCreationPromises.get(requestId)?.reject(new Error("Empty run creation timed out."));
              this.runCreationPromises.delete(requestId);
          }
      }, 15000); // 15-second timeout
    });
  }



  // Method to send stop_managed_principal message
  async stopManagedPrincipal(managingPartnerRunId: string) {
    if (this.ws?.readyState === WebSocket.OPEN) {
      const payload = {
        type: 'stop_managed_principal',
        data: {
          managing_partner_run_id: managingPartnerRunId
        }
      };
      this.ws.send(JSON.stringify(payload));
      console.log('Sent stop_managed_principal:', payload);
    } else {
      console.error('WebSocket is not open. Cannot stop managed principal.');
      this.error = 'WebSocket is not connected. Please try connecting again.';
    }
  }

  // Method to send request_available_toolsets message
  async requestAvailableToolsets() {
    if (this.ws?.readyState === WebSocket.OPEN) {
      const payload = {
        type: 'request_available_toolsets',
        data: {}
      };
      this.ws.send(JSON.stringify(payload));
      console.log('Sent request_available_toolsets:', payload);
    } else {
      console.error('WebSocket is not open. Cannot request available toolsets.');
      this.error = 'WebSocket is not connected. Please try connecting again.';
    }
  }

  // region: ViewModel Subscription and Management
  subscribeToView(runId: string, viewName: 'flow_view' | 'kanban_view' | 'timeline_view') {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({
        type: 'subscribe_to_view',
        data: { run_id: runId, view_name: viewName }
      }));
    }
  }

  unsubscribeFromView(runId: string, viewName: 'flow_view' | 'kanban_view' | 'timeline_view') {
    if (this.ws?.readyState === WebSocket.OPEN) {
        this.ws.send(JSON.stringify({
            type: 'unsubscribe_from_view',
            data: { run_id: runId, view_name: viewName }
        }));
    }
  }

  manageWorkModulesRequest(runId: string, actions: WorkModuleAction[]) {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({
        type: 'manage_work_modules_request',
        data: { run_id: runId, actions }
      }));
    }
  }

  findNodeInFlow(nodeId: string): FlowNode | undefined {
    if (!this.flowStructure) return undefined;

    // The node ID in the detail view might be the aggregated 'turn-...' ID.
    // We need to find the original node that corresponds to it.
    if (nodeId.startsWith('turn-')) {
      const turnId = nodeId.replace('turn-', '');
      return this.flowStructure.nodes.find(n => n.data.turn_id === turnId);
    }

    return this.flowStructure.nodes.find(n => n.id === nodeId);
  }

  cleanup() {
    this.stopStreamingProcessor();
    if (this.ws) {
      console.log('Closing WebSocket connection');
      this.ws.close();
      this.ws = null;
      this.isConnected = false;
      this.isConnecting = false;
    }
  }
  // endregion
}

interface WorkModuleAction {
  action: 'add' | 'update' | 'delete';
  details: Record<string, unknown>;
}

export const sessionStore = new SessionStore();
