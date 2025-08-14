import React, { useState, useRef, useCallback, useEffect } from 'react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { X, Paperclip, Music, Video, FileText } from 'lucide-react';


interface FileAttachment {
  id: string;
  file: File;
  dataUrl: string;
  name: string;
  mimeType?: string;
  kind?: 'image' | 'audio' | 'video' | 'document';
}

interface ChatInputProps {
  currentInput: string;
  onInputChange: (value: string) => void;
  onKeyPress: (e: React.KeyboardEvent) => void;
  onSendMessage: (files?: FileAttachment[]) => void;
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
  const [files, setFiles] = useState<FileAttachment[]>([]);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);
  const errorTimerRef = useRef<number | null>(null);

  const showError = useCallback((msg: string) => {
    setErrorMsg(msg);
    if (errorTimerRef.current) {
      window.clearTimeout(errorTimerRef.current);
    }
    errorTimerRef.current = window.setTimeout(() => {
      setErrorMsg(null);
      errorTimerRef.current = null;
    }, 3000);
  }, []);

  // Convert File to base64 data URL (for backward compatibility on send)
  const fileToDataUrl = useCallback((file: File) => {
    return new Promise<string>((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => resolve(reader.result as string);
      reader.onerror = reject;
      reader.readAsDataURL(file);
    });
  }, []);

  useEffect(() => {
    return () => {
      if (errorTimerRef.current) {
        window.clearTimeout(errorTimerRef.current);
      }
      // Revoke any remaining object URLs on unmount to prevent memory leaks
      try {
        files.forEach(file => {
          if (file.kind === 'image' && file.dataUrl) {
            URL.revokeObjectURL(file.dataUrl);
          }
        });
      } catch {}
    };
  }, [files]);

  // 粘贴事件处理在下方定义，以确保依赖的 addFile 已声明

  // Add file (image/audio/video/document)
  const addFile = useCallback((file: File) => {
    const isImage = file.type.startsWith('image/');
    const isAudio = file.type.startsWith('audio/');
    const isVideo = file.type.startsWith('video/');
  // Paste event handler is defined below to ensure addFile is already declared

  // Add file (image/audio/video/document)
  const addFile = useCallback((file: File) => {
    const isImage = file.type.startsWith('image/');
    const isAudio = file.type.startsWith('audio/');
    const isVideo = file.type.startsWith('video/');
    // Document type: application/* or text/*, or fallback by extension
    const ext = file.name.split('.').pop()?.toLowerCase() || '';
    const docExts = new Set(['pdf','doc','docx','xls','xlsx','ppt','pptx','txt','rtf','md','csv']);
    const isDocByMime = file.type.startsWith('application/') || file.type.startsWith('text/');
    const isDocument = (!isImage && !isAudio && !isVideo) && (isDocByMime || docExts.has(ext));

    if (!isImage && !isAudio && !isVideo && !isDocument) {
      showError('Unsupported file type. Allowed: images, audio, video, documents.');
      return;
    }

    // 允许超过 20MB 的文件添加；
    // 小于 20MB 的所有文件（图片/音频/视频/文本/文档）在发送时统一转换为 dataUrl，
    // Allow files larger than 20MB to be added;
    // All files smaller than 20MB (images/audio/video/text/documents) will be converted to dataUrl when sending,
    // Files 20MB or larger will be sent to the backend for upload as files.

    // Generate Blob URL preview for images; for audio, video, and documents, only keep the file and name
    const objectUrl = isImage ? URL.createObjectURL(file) : '';
    const newAttachment: FileAttachment = {
      id: crypto.randomUUID(),
      file,
      dataUrl: objectUrl,
      name: file.name,
      mimeType: file.type,
      kind: isImage ? 'image' : isAudio ? 'audio' : isVideo ? 'video' : 'document',
    };
    setFiles(prev => [...prev, newAttachment]);
  }, [showError]);

  // Handle paste event (only supports images)
  const handlePaste = useCallback((e: React.ClipboardEvent) => {
    const items = e.clipboardData?.items;
    if (!items) return;

    for (let i = 0; i < items.length; i++) {
      const item = items[i];
      if (item.type.startsWith('image/')) {
        e.preventDefault();
        const file = item.getAsFile();
        if (file) {
          addFile(file);
        }
        break;
      }
    }
  }, [addFile]);

  // Handle file selection
  const handleFileSelect = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    const files = e.target.files;
    if (files) {
      Array.from(files).forEach(addFile);
    }
    // Clear the input value to allow selecting the same file again
    if (fileInputRef.current) {
      fileInputRef.current.value = '';
    }
  }, [addFile]);

  // 移除图像
  const removeFile = useCallback((id: string) => {
    setFiles(prev => {
      const target = prev.find(img => img.id === id);
      if (target) {
        try { if (target.kind === 'image' && target.dataUrl) { URL.revokeObjectURL(target.dataUrl); } } catch {}
      }
      return prev.filter(img => img.id !== id);
    });
  }, []);

  // 处理发送消息
  const handleSendMessage = useCallback(async () => {
    if (files.length > 0) {
      // Prefer file; convert to base64 dataUrl for all files <20MB (image/audio/video/text/document)
      const converted = await Promise.all(files.map(async (img) => {
        const kind = img.kind ?? 'image';
        const isImage = kind === 'image';
        const isAudio = kind === 'audio';
        const isVideo = kind === 'video';
        const isDocument = kind === 'document';
        const isTextMime = (img.mimeType ?? '').startsWith('text/');
        const shouldConvert = (isImage || isAudio || isVideo || isTextMime || isDocument) && img.file.size < 20 * 1024 * 1024;
        return {
          ...img,
          dataUrl: shouldConvert ? await fileToDataUrl(img.file) : '',
        };
      }));
      onSendMessage(converted);
    } else {
      onSendMessage(undefined);
    }
    // Revoke all object URLs and clear after sending
    files.forEach(file => {
      try { URL.revokeObjectURL(file.dataUrl); } catch {}
    });
    setFiles([]);
  }, [onSendMessage, files, fileToDataUrl]);

  // 处理键盘事件
  const handleKeyPress = useCallback((e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      if ((currentInput.trim() || files.length > 0) && !isStreaming && !isLoading) {
        handleSendMessage();
      }
    } else {
      onKeyPress(e);
    }
  }, [currentInput, files, isStreaming, isLoading, handleSendMessage, onKeyPress]);

  return (
    <div className="p-3">
      {/* 附件预览区域 */}
      {files.length > 0 && (
        <div className="mb-3 p-3 bg-gray-50 rounded-lg">
          <div className="flex flex-wrap gap-2">
            {files.map((file) => (
              <div key={file.id} className="relative group">
                {file.kind === 'image' && file.dataUrl ? (
                  <img
                    src={file.dataUrl}
                    alt={file.name}
                    className="w-20 h-20 object-cover rounded border"
                  />
                ) : (
                  <div className="w-20 h-20 rounded border bg-white flex items-center justify-center">
                    {file.kind === 'audio' ? <Music size={18} /> : file.kind === 'video' ? <Video size={18} /> : <FileText size={18} />}
                  </div>
                )}
                <button
                  onClick={() => removeFile(file.id)}
                  className="absolute -top-1 -right-1 w-5 h-5 bg-red-500 text-white rounded-full flex items-center justify-center opacity-0 group-hover:opacity-100 transition-opacity"
                  aria-label="Remove attachment"
                >
                  <X size={12} />
                </button>
                <div className="absolute bottom-0 left-0 right-0 bg-black bg-opacity-50 text-white text-xs p-1 rounded-b truncate">
                  {file.name}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {errorMsg && (
        <div className="mb-2 text-sm text-red-600 bg-red-50 border border-red-200 rounded px-2 py-1">
          {errorMsg}
        </div>
      )}

      <div
        className="flex gap-2 bg-white rounded-lg border overflow-hidden p-2 focus-within:border-black transition-colors"
        onDragOver={(e) => { e.preventDefault(); e.stopPropagation(); }}
        onDrop={(e) => {
          e.preventDefault();
          e.stopPropagation();
          const files = Array.from(e.dataTransfer.files || []);
          files.forEach((f) => addFile(f));
        }}
      >
        {/* 文件上传按钮 */}
        <Button
          variant="ghost"
          size="icon"
          onClick={() => fileInputRef.current?.click()}
          disabled={isStreaming || isLoading}
          className="flex-shrink-0"
          aria-label="Attach files"
        >
          <Paperclip size={16} />
        </Button>
        
        <Input
          ref={inputRef}
          value={currentInput}
          onChange={(e) => onInputChange(e.target.value)}
          onKeyPress={handleKeyPress}
          onPaste={handlePaste}
          placeholder="Enter message, paste or drag-and-drop files (images/audio/video/docs)..."
          disabled={isStreaming || isLoading}
          className="flex-1 border-0 focus-visible:ring-0 focus-visible:ring-offset-0 shadow-none px-2"
        />
        
        {isStreaming ? (
          <Button
            variant="ghost"
            size="icon"
            className="rounded-full bg-black hover:bg-black/90 !px-2 !py-1 flex-shrink-0"
            onClick={onStopExecution}
            aria-label="Stop streaming"
          >
            <div className="w-3 h-3 bg-white" />
          </Button>
        ) : (
          <Button 
            onClick={handleSendMessage}
            disabled={(!currentInput.trim() && files.length === 0) || isLoading}
            className="flex-shrink-0"
            aria-label="Send message"
          >
            Send
          </Button>
        )}
      </div>
      
      {/* 隐藏的文件输入 */}
      <input
        ref={fileInputRef}
        type="file"
        accept="image/*,audio/*,video/*,.pdf,.doc,.docx,.xls,.xlsx,.ppt,.pptx,.txt,.rtf,.md,.csv"
        multiple
        onChange={handleFileSelect}
        className="hidden"
      />
    </div>
  );
}
