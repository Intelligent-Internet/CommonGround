'use client';

import React, { useEffect, useState, useCallback, useContext } from 'react';
import { observer } from 'mobx-react-lite';
import { useLocalObservable } from 'mobx-react-lite';
import { useRouter } from 'next/navigation';
import { sessionStore } from '@/app/stores/sessionStore';
import { selectionStore } from '@/app/stores/selectionStore';
import LoadingSpinner from '@/components/layout/LoadingSpinner';
import { SidebarContext } from '@/components/ui/sidebar';
import { WelcomeScreen } from './components/WelcomeScreen';

export default observer(function ChatPage() {
  const [isClient, setIsClient] = useState(false);
  const sidebarContext = useContext(SidebarContext);
  const router = useRouter();

  const store = useLocalObservable(() => ({
    currentInput: '',
    isLoading: false,
    isCreatingRun: false,
    setCurrentInput(text: string) { this.currentInput = text; },
    setIsLoading(loading: boolean) { this.isLoading = loading; },
    setIsCreatingRun(creating: boolean) { this.isCreatingRun = creating; }
  }));

  useEffect(() => { 
    setIsClient(true);
    // Clear selection state in selectionStore for a new chat session.
    selectionStore.clearSelection();
  }, []);

  const sendMessage = useCallback(async (files?: any[]) => {
    if ((!store.currentInput.trim() && !files?.length) || store.isLoading || store.isCreatingRun) return;
    
    // Close the sidebar (if on mobile or first message)
    if (sidebarContext) {
      if (sidebarContext.isMobile) {
        sidebarContext.setOpenMobile(false);
      } else {
        sidebarContext.setOpen(false);
      }
    }
    
    const messageToSend = store.currentInput;
    store.setCurrentInput('');
    
    try {
        store.setIsCreatingRun(true);
        
        console.log('Creating new run in default project');

        // Create a new run in the default project.
        const newRunId = await sessionStore.createRun('partner_interaction', 'default');
        
        // Send message
        store.setIsLoading(true);
        await sessionStore.sendMessage(messageToSend, newRunId, files);

        // Redirect to the newly created run page
        router.push(`/r?id=${newRunId}`);

    } catch (error) {
        console.error('Failed to create run or send message:', error);
        if (error instanceof Error) {
          sessionStore.error = error.message;
        } else {
          sessionStore.error = 'An unknown error occurred.';
        }
    } finally {
        store.setIsLoading(false);
        store.setIsCreatingRun(false);
    }
  }, [store, sidebarContext, router]);

  const handleKeyPress = useCallback((e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
  }, [sendMessage]);

  if (!isClient) {
    return (
      <div className="h-screen flex items-center justify-center bg-background">
        <LoadingSpinner />
      </div>
    );
  }

  return (
    <WelcomeScreen
      currentInput={store.currentInput}
      onInputChange={(val) => store.setCurrentInput(val)}
      onSendMessage={sendMessage}
      onKeyPress={handleKeyPress}
      isLoading={store.isLoading || store.isCreatingRun}
    />
  );
});
