import React from 'react';
import { observer } from 'mobx-react-lite';
import { SidebarTrigger } from '@/components/ui/sidebar';
import { selectionStore } from '@/app/stores/selectionStore';
import { ChatInput } from './ChatInput';

interface WelcomeScreenProps {
  currentInput: string;
  onInputChange: (value: string) => void;
  onSendMessage: (files?: any[]) => void;
  onKeyPress: (e: React.KeyboardEvent) => void;
  isLoading: boolean;
}

export const WelcomeScreen = observer(function WelcomeScreen({ currentInput, onInputChange, onSendMessage, onKeyPress, isLoading }: WelcomeScreenProps) {
  return (
    <div className="flex flex-col h-screen">
      <div className="flex-shrink-0 h-12 bg-white flex items-center justify-between px-3">
        <div className="flex items-center gap-2">
          <SidebarTrigger />
          <div className="flex items-center gap-2 text-sm">
            <span className="font-medium">{selectionStore.displayProjectName}</span>
            <span className="text-gray-400">&gt;</span>
            <span>{selectionStore.displayFileName}</span>
          </div>
        </div>
      </div>
      <div className="flex-1 flex flex-col items-center justify-center">
        <h1 className="text-[32px] font-medium mb-12">What can I help you with?</h1>
        <div className="w-[600px] mb-12">
          <ChatInput
            currentInput={currentInput}
            onInputChange={onInputChange}
            onKeyPress={onKeyPress}
            onSendMessage={onSendMessage}
            isStreaming={false}
            isLoading={isLoading}
            onStopExecution={() => {}}
          />
        </div>
        {/* <div className="flex flex-col items-start justify-center">
          <p className="font-medium text-lg">From the Community</p>
          <p className="text-[#767575] text-lg mb-4">Explore what the community is building with Common Ground.</p>
          <div className="flex gap-6">
            <div className="w-[280px] h-[180px] bg-gray-100 rounded-lg" />
            <div className="w-[280px] h-[180px] bg-gray-100 rounded-lg" />
            <div className="w-[280px] h-[180px] bg-gray-100 rounded-lg" />
          </div>
        </div> */}
      </div>
    </div>
  );
});
