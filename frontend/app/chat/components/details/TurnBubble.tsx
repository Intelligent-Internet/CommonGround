import React from 'react';
import { observer } from 'mobx-react-lite';
import { sessionStore, Turn } from '@/app/stores/sessionStore';
import { Card, CardContent } from '@/components/ui/card';
import { Avatar, AvatarFallback } from '@/components/ui/avatar';
import { formatMessageTime } from '@/app/utils/time';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { Code, Info } from 'lucide-react';
import { Badge } from '@/components/ui/badge';
import { Tooltip, TooltipTrigger, TooltipContent, TooltipProvider } from '@/components/ui/tooltip';
import { ThinkingDots } from './ThinkingDots';

interface ToolInteractionData {
  tool_name: string;
  status: 'running' | 'completed' | 'error' | string;
  input_params?: Record<string, unknown>;
  result_payload?: unknown;
  tool_call_id: string;
}

type ParamsValue = string | number | boolean | null | ParamsValue[] | { [key: string]: ParamsValue };

interface ParamsListProps {
  data: Record<string, ParamsValue> | ParamsValue[];
}

const JsonViewer = ({ data }: { data: unknown }) => (
  <pre className="bg-gray-900 text-white p-2 rounded-md text-xs whitespace-pre-wrap break-words">
    {JSON.stringify(data, null, 2)}
  </pre>
);

const ParamsList = ({ data }: ParamsListProps) => {
  const renderValue = (value: ParamsValue) => {
    if (typeof value !== 'object' || value === null) {
      return <span className="font-mono bg-gray-200 px-1 rounded">{String(value)}</span>;
    }
    return <ParamsList data={value} />;
  };

  if (Array.isArray(data)) {
    return (
      <ul className="list-disc pl-5 mt-1">
        {data.map((item, index) => <li key={index}>{renderValue(item)}</li>)}
      </ul>
    );
  }

  // It's an object
  return (
    <ul className="list-disc pl-4 space-y-1">
      {Object.entries(data).map(([key, value]) => (
        <li key={key}>
          <strong>{key}:</strong> {renderValue(value)}
        </li>
      ))}
    </ul>
  );
};

const ToolInteraction = ({ interaction }: { interaction: ToolInteractionData }) => {
  // Check if there are renderable input parameters
  const hasInputParams = interaction.input_params && Object.keys(interaction.input_params).length > 0;
  
  // Check if there is a renderable result
  const hasResultPayload = interaction.result_payload !== undefined && interaction.result_payload !== null;

  return (
    <div className="mt-2 border-t pt-2 space-y-2">
      <div className="flex items-center justify-between">
        <span className="font-semibold text-xs">{interaction.tool_name}</span>
        <Badge variant={
          interaction.status === 'running' ? 'default' :
          interaction.status.startsWith('completed') ? 'secondary' :
          interaction.status.includes('error') ? 'destructive' : 'outline'
        } className={`text-xs ${
          interaction.status === 'running' ? 'animate-pulse' : ''
        }`}>
          {interaction.status}
        </Badge>
      </div>

      {/* Added: Display input parameters */}
      {hasInputParams && interaction.input_params && (
        <div className="text-xs text-gray-700 bg-gray-50 p-2 rounded-md">
          <ParamsList data={interaction.input_params as Record<string, ParamsValue>} />
        </div>
      )}

      {/* Added: Display collapsible results */}
      {hasResultPayload && (
        <Collapsible>
          <CollapsibleTrigger className="text-xs text-blue-600 hover:underline">
            View Tool Result
          </CollapsibleTrigger>
          <CollapsibleContent className="mt-2">
            <JsonViewer data={interaction.result_payload} />
          </CollapsibleContent>
        </Collapsible>
      )}
    </div>
  );
};

interface TurnBubbleProps {
  turn: Turn;
  isHighlighted?: boolean;
  onNodeIdClick?: (nodeId: string) => void;
}

export const TurnBubble = observer(({ turn, isHighlighted = false, onNodeIdClick }: TurnBubbleProps) => {
  const llmInteraction = turn.llm_interaction;
  const tokenUsage = llmInteraction?.actual_usage;
  const isUserTurn = turn.agent_info.agent_id === 'User';
  
  const agentDisplayName = turn.agent_info.assigned_role_name || turn.agent_info.agent_id;

  const streamingContent = !isUserTurn && llmInteraction?.attempts && llmInteraction.attempts.length > 0
    ? sessionStore.streamingContent.get(llmInteraction.attempts[llmInteraction.attempts.length - 1].stream_id) 
    : null;
  
  let finalContent = '';
  if (isUserTurn) {
    finalContent = turn.inputs?.prompt || '';
  } else {
    finalContent = llmInteraction?.final_response?.content || '';
  }

  // When running, prioritize streaming content. When finished, show only final content.
  const displayContent = turn.status === 'running' 
    ? (streamingContent || finalContent) 
    : finalContent;

  const handleNodeClick = onNodeIdClick ? () => onNodeIdClick(turn.turn_id) : undefined;

  return (
    <div id={turn.turn_id} className={`flex flex-col w-full ${isUserTurn ? 'items-end' : 'items-start'} ${isHighlighted ? 'bg-yellow-100 rounded-lg p-1 transition-colors duration-500' : ''}`}>
      <div className={`flex flex-col ${isUserTurn ? 'max-w-[80%]' : 'max-w-3xl'}`}>
        <div className={`flex items-center gap-2 flex-row ${isUserTurn ? 'flex-row-reverse' : ''}`}>
          <Avatar className="h-6 w-6">
            <AvatarFallback>{agentDisplayName.charAt(0)}</AvatarFallback>
          </Avatar>
          <span className="font-bold text-sm">{agentDisplayName}</span>
          {turn.status === 'running' && <ThinkingDots className="text-gray-500" />}
          <span 
            className={`text-xs text-gray-400 ${handleNodeClick ? 'hover:underline cursor-pointer' : ''}`}
            onClick={handleNodeClick}
            title={handleNodeClick ? `Click to highlight turn ${turn.turn_id} in FlowView` : `Turn started at ${new Date(turn.start_time).toLocaleTimeString()}`}
          >
            {formatMessageTime(new Date(turn.start_time))}
          </span>
          {tokenUsage && (
              <TooltipProvider delayDuration={100}>
                  <Tooltip>
                      <TooltipTrigger>
                          <Info size={12} className="text-gray-400 hover:text-gray-600" />
                      </TooltipTrigger>
                      <TooltipContent side="top" align="center">
                          <p className="text-xs">
                              Send: {tokenUsage.prompt_tokens} tokens / 
                              Receive: {tokenUsage.completion_tokens} tokens
                          </p>
                      </TooltipContent>
                  </Tooltip>
              </TooltipProvider>
          )}
        </div>
        <Card className={`mt-1 ${isUserTurn ? 'bg-gray-50' : 'bg-white border-0 shadow-none'}`}>
          <CardContent className="p-3">
            <div className="prose prose-sm max-w-none break-words space-y-2">
              {displayContent && (
                <ReactMarkdown remarkPlugins={[remarkGfm]}>{displayContent}</ReactMarkdown>
              )}
              {turn.tool_interactions?.map((interaction) => (
                <ToolInteraction key={interaction.tool_call_id} interaction={interaction} />
              ))}
            </div>
            {!isUserTurn && (
              <Collapsible className="mt-2">
                <CollapsibleTrigger className="flex items-center gap-1 text-xs text-gray-500 hover:text-black">
                  <Code size={14} />
                  <span>View Raw Turn Data</span>
                </CollapsibleTrigger>
                <CollapsibleContent className="mt-2">
                  <JsonViewer data={turn} />
                </CollapsibleContent>
              </Collapsible>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
});
