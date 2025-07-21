
import React from 'react';
import { cn } from '@/lib/utils';

interface ThinkingDotsProps {
  className?: string;
}

export const ThinkingDots = ({ className }: ThinkingDotsProps) => {
  return (
    <div className={cn("flex items-center gap-1", className)}>
      <span 
        className="w-1.5 h-1.5 bg-current rounded-full animate-blink" 
        style={{ animationDelay: '0s' }} 
      />
      <span 
        className="w-1.5 h-1.5 bg-current rounded-full animate-blink" 
        style={{ animationDelay: '0.2s' }} 
      />
      <span 
        className="w-1.5 h-1.5 bg-current rounded-full animate-blink" 
        style={{ animationDelay: '0.4s' }} 
      />
    </div>
  );
};
