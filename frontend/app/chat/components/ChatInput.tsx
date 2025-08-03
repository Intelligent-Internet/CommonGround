import React, { useState, useRef, useCallback } from 'react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { X, Image as ImageIcon, Paperclip } from 'lucide-react';

interface ImageAttachment {
  id: string;
  file: File;
  dataUrl: string;
  name: string;
}

interface ChatInputProps {
  currentInput: string;
  onInputChange: (value: string) => void;
  onKeyPress: (e: React.KeyboardEvent) => void;
  onSendMessage: (images?: ImageAttachment[]) => void;
  isStreaming: boolean;
  isLoading: boolean;
  onStopExecution: () => void;
}

export function ChatInput({
  currentInput,
  onInputChange,
  onKeyPress,
  onSendMessage,
  isStreaming,
  isLoading,
  onStopExecution,
}: ChatInputProps) {
  const [images, setImages] = useState<ImageAttachment[]>([]);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  // 处理粘贴事件
  const handlePaste = useCallback((e: React.ClipboardEvent) => {
    const items = e.clipboardData?.items;
    if (!items) return;

    for (let i = 0; i < items.length; i++) {
      const item = items[i];
      if (item.type.startsWith('image/')) {
        e.preventDefault();
        const file = item.getAsFile();
        if (file) {
          addImageFile(file);
        }
        break;
      }
    }
  }, []);

  // 添加图像文件
  const addImageFile = useCallback((file: File) => {
    if (!file.type.startsWith('image/')) {
      alert('Please select an image file');
      return;
    }

    // 限制文件大小为10MB
    if (file.size > 10 * 1024 * 1024) {
      alert('Image size should be less than 10MB');
      return;
    }

    const reader = new FileReader();
    reader.onload = (e) => {
      const dataUrl = e.target?.result as string;
      const newImage: ImageAttachment = {
        id: Date.now().toString(),
        file,
        dataUrl,
        name: file.name
      };
      setImages(prev => [...prev, newImage]);
    };
    reader.readAsDataURL(file);
  }, []);

  // 处理文件选择
  const handleFileSelect = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    const files = e.target.files;
    if (files) {
      Array.from(files).forEach(addImageFile);
    }
    // 清空input值以允许重复选择同一文件
    if (fileInputRef.current) {
      fileInputRef.current.value = '';
    }
  }, [addImageFile]);

  // 移除图像
  const removeImage = useCallback((id: string) => {
    setImages(prev => prev.filter(img => img.id !== id));
  }, []);

  // 处理发送消息
  const handleSendMessage = useCallback(() => {
    onSendMessage(images.length > 0 ? images : undefined);
    setImages([]); // 发送后清空图像
  }, [onSendMessage, images]);

  // 处理键盘事件
  const handleKeyPress = useCallback((e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      if ((currentInput.trim() || images.length > 0) && !isStreaming && !isLoading) {
        handleSendMessage();
      }
    } else {
      onKeyPress(e);
    }
  }, [currentInput, images, isStreaming, isLoading, handleSendMessage, onKeyPress]);

  return (
    <div className="p-3">
      {/* 图像预览区域 */}
      {images.length > 0 && (
        <div className="mb-3 p-3 bg-gray-50 rounded-lg">
          <div className="flex flex-wrap gap-2">
            {images.map((image) => (
              <div key={image.id} className="relative group">
                <img
                  src={image.dataUrl}
                  alt={image.name}
                  className="w-20 h-20 object-cover rounded border"
                />
                <button
                  onClick={() => removeImage(image.id)}
                  className="absolute -top-1 -right-1 w-5 h-5 bg-red-500 text-white rounded-full flex items-center justify-center opacity-0 group-hover:opacity-100 transition-opacity"
                >
                  <X size={12} />
                </button>
                <div className="absolute bottom-0 left-0 right-0 bg-black bg-opacity-50 text-white text-xs p-1 rounded-b truncate">
                  {image.name}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      <div className="flex gap-2 bg-white rounded-lg border overflow-hidden p-2 focus-within:border-black transition-colors">
        {/* 文件上传按钮 */}
        <Button
          variant="ghost"
          size="icon"
          onClick={() => fileInputRef.current?.click()}
          disabled={isStreaming || isLoading}
          className="flex-shrink-0"
        >
          <Paperclip size={16} />
        </Button>
        
        <Input
          ref={inputRef}
          value={currentInput}
          onChange={(e) => onInputChange(e.target.value)}
          onKeyPress={handleKeyPress}
          onPaste={handlePaste}
          placeholder="Enter message or paste image..."
          disabled={isStreaming || isLoading}
          className="flex-1 border-0 focus-visible:ring-0 focus-visible:ring-offset-0 shadow-none px-2"
        />
        
        {isStreaming ? (
          <Button
            variant="ghost"
            size="icon"
            className="rounded-full bg-black hover:bg-black/90 !px-2 !py-1 flex-shrink-0"
            onClick={onStopExecution}
          >
            <div className="w-3 h-3 bg-white" />
          </Button>
        ) : (
          <Button 
            onClick={handleSendMessage}
            disabled={(!currentInput.trim() && images.length === 0) || isLoading}
            className="flex-shrink-0"
          >
            Send
          </Button>
        )}
      </div>
      
      {/* 隐藏的文件输入 */}
      <input
        ref={fileInputRef}
        type="file"
        accept="image/*"
        multiple
        onChange={handleFileSelect}
        className="hidden"
      />
    </div>
  );
}
