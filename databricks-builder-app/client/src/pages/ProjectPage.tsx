import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { useUser } from '@/contexts/UserContext';
import {
  Brain,
  ChevronDown,
  ChevronRight,
  ExternalLink,
  Loader2,
  MessageSquare,
  Send,
  Terminal,
  Wrench,
} from 'lucide-react';
import { toast } from 'sonner';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { MainLayout } from '@/components/layout/MainLayout';
import { Sidebar } from '@/components/layout/Sidebar';
import { Button } from '@/components/ui/Button';
import {
  createConversation,
  deleteConversation,
  fetchClusters,
  fetchConversation,
  fetchConversations,
  fetchProject,
  invokeAgent,
} from '@/lib/api';
import type { Cluster, Conversation, Message, Project } from '@/lib/types';
import { cn } from '@/lib/utils';

// Combined activity item for display
interface ActivityItem {
  id: string;
  type: 'thinking' | 'tool_use' | 'tool_result';
  content: string;
  toolName?: string;
  toolInput?: Record<string, unknown>;
  isError?: boolean;
  timestamp: number;
}

// Collapsible activity section component
function ActivitySection({
  items,
  isStreaming,
}: {
  items: ActivityItem[];
  isStreaming: boolean;
}) {
  const [isExpanded, setIsExpanded] = useState(true);

  if (items.length === 0) return null;

  return (
    <div className="mb-4 rounded-lg border border-[var(--color-border)]/50 bg-[var(--color-bg-secondary)]/30 overflow-hidden">
      <button
        onClick={() => setIsExpanded(!isExpanded)}
        className="flex w-full items-center gap-2 px-3 py-2 text-xs font-medium text-[var(--color-text-muted)] hover:bg-[var(--color-bg-secondary)]/50 transition-colors"
      >
        {isExpanded ? (
          <ChevronDown className="h-3 w-3" />
        ) : (
          <ChevronRight className="h-3 w-3" />
        )}
        <Brain className="h-3 w-3" />
        <span>Activity ({items.length} events)</span>
        {isStreaming && (
          <Loader2 className="h-3 w-3 animate-spin ml-auto" />
        )}
      </button>

      {isExpanded && (
        <div className="border-t border-[var(--color-border)]/30 max-h-64 overflow-y-auto">
          {items.map((item) => (
            <div
              key={item.id}
              className={cn(
                'px-3 py-2 text-xs border-b border-[var(--color-border)]/20 last:border-0',
                item.type === 'thinking' && 'bg-purple-500/5',
                item.type === 'tool_use' && 'bg-blue-500/5',
                item.type === 'tool_result' && (item.isError ? 'bg-red-500/5' : 'bg-green-500/5')
              )}
            >
              <div className="flex items-center gap-2 mb-1">
                {item.type === 'thinking' && (
                  <>
                    <Brain className="h-3 w-3 text-purple-500" />
                    <span className="font-medium text-purple-600">Thinking</span>
                  </>
                )}
                {item.type === 'tool_use' && (
                  <>
                    <Wrench className="h-3 w-3 text-blue-500" />
                    <span className="font-medium text-blue-600">
                      {item.toolName}
                    </span>
                  </>
                )}
                {item.type === 'tool_result' && (
                  <>
                    <Terminal className="h-3 w-3 text-green-500" />
                    <span className={cn(
                      'font-medium',
                      item.isError ? 'text-red-600' : 'text-green-600'
                    )}>
                      Result {item.isError && '(error)'}
                    </span>
                  </>
                )}
              </div>
              <div className="text-[var(--color-text-muted)] font-mono whitespace-pre-wrap break-all">
                {item.type === 'tool_use' && item.toolInput ? (
                  <code className="text-[10px]">
                    {JSON.stringify(item.toolInput, null, 2).slice(0, 500)}
                    {JSON.stringify(item.toolInput).length > 500 && '...'}
                  </code>
                ) : (
                  <span>
                    {item.content.slice(0, 300)}
                    {item.content.length > 300 && '...'}
                  </span>
                )}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// Convert email to schema name: quentin.ambard@databricks.com -> quentin_ambard
function emailToSchemaName(email: string | null): string {
  if (!email) return '';
  const localPart = email.split('@')[0];
  return localPart.replace(/[^a-zA-Z0-9]/g, '_').toLowerCase();
}

export default function ProjectPage() {
  const { projectId } = useParams<{ projectId: string }>();
  const navigate = useNavigate();
  const { user, workspaceUrl } = useUser();

  // State
  const [project, setProject] = useState<Project | null>(null);
  const [conversations, setConversations] = useState<Conversation[]>([]);
  const [currentConversation, setCurrentConversation] = useState<Conversation | null>(null);
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState('');
  const [isLoading, setIsLoading] = useState(true);
  const [isStreaming, setIsStreaming] = useState(false);
  const [streamingText, setStreamingText] = useState('');
  const [activityItems, setActivityItems] = useState<ActivityItem[]>([]);
  const [clusters, setClusters] = useState<Cluster[]>([]);
  const [selectedClusterId, setSelectedClusterId] = useState<string | undefined>();
  const [clusterDropdownOpen, setClusterDropdownOpen] = useState(false);
  const [defaultCatalog, setDefaultCatalog] = useState<string>('ai_dev_kit');
  const [defaultSchema, setDefaultSchema] = useState<string>('');

  // Calculate default schema from user email once available
  const userDefaultSchema = useMemo(() => emailToSchemaName(user), [user]);

  // Refs
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);
  const abortControllerRef = useRef<AbortController | null>(null);
  const clusterDropdownRef = useRef<HTMLDivElement>(null);

  // Load project and conversations
  useEffect(() => {
    if (!projectId) return;

    const loadData = async () => {
      try {
        setIsLoading(true);
        const [projectData, conversationsData, clustersData] = await Promise.all([
          fetchProject(projectId),
          fetchConversations(projectId),
          fetchClusters().catch(() => []), // Don't fail if clusters can't be loaded
        ]);
        setProject(projectData);
        setConversations(conversationsData);
        setClusters(clustersData);

        // Load first conversation if available
        if (conversationsData.length > 0) {
          const conv = await fetchConversation(projectId, conversationsData[0].id);
          setCurrentConversation(conv);
          setMessages(conv.messages || []);
          // Restore cluster selection from conversation, or default to first cluster
          if (conv.cluster_id) {
            setSelectedClusterId(conv.cluster_id);
          } else if (clustersData.length > 0) {
            setSelectedClusterId(clustersData[0].cluster_id);
          }
          // Restore catalog/schema from conversation
          if (conv.default_catalog) {
            setDefaultCatalog(conv.default_catalog);
          }
          if (conv.default_schema) {
            setDefaultSchema(conv.default_schema);
          }
        } else if (clustersData.length > 0) {
          // No conversation yet, but still select first cluster
          setSelectedClusterId(clustersData[0].cluster_id);
        }
      } catch (error) {
        console.error('Failed to load project:', error);
        toast.error('Failed to load project');
        navigate('/');
      } finally {
        setIsLoading(false);
      }
    };

    loadData();
  }, [projectId, navigate]);

  // Scroll to bottom when messages change
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, streamingText, activityItems]);

  // Close cluster dropdown on outside click
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (clusterDropdownRef.current && !clusterDropdownRef.current.contains(event.target as Node)) {
        setClusterDropdownOpen(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  // Set default schema from user email if not already set
  useEffect(() => {
    if (userDefaultSchema && !defaultSchema) {
      setDefaultSchema(userDefaultSchema);
    }
  }, [userDefaultSchema, defaultSchema]);

  // Select a conversation
  const handleSelectConversation = async (conversationId: string) => {
    if (!projectId || currentConversation?.id === conversationId) return;

    try {
      const conv = await fetchConversation(projectId, conversationId);
      setCurrentConversation(conv);
      setMessages(conv.messages || []);
      setActivityItems([]);
      // Restore cluster selection from conversation, or default to first cluster
      setSelectedClusterId(conv.cluster_id || (clusters.length > 0 ? clusters[0].cluster_id : undefined));
      // Restore catalog/schema from conversation, or use defaults
      setDefaultCatalog(conv.default_catalog || 'ai_dev_kit');
      setDefaultSchema(conv.default_schema || userDefaultSchema);
    } catch (error) {
      console.error('Failed to load conversation:', error);
      toast.error('Failed to load conversation');
    }
  };

  // Create new conversation
  const handleNewConversation = async () => {
    if (!projectId) return;

    try {
      const conv = await createConversation(projectId);
      setConversations((prev) => [conv, ...prev]);
      setCurrentConversation(conv);
      setMessages([]);
      setActivityItems([]);
      inputRef.current?.focus();
    } catch (error) {
      console.error('Failed to create conversation:', error);
      toast.error('Failed to create conversation');
    }
  };

  // Delete conversation
  const handleDeleteConversation = async (conversationId: string) => {
    if (!projectId) return;

    try {
      await deleteConversation(projectId, conversationId);
      setConversations((prev) => prev.filter((c) => c.id !== conversationId));

      if (currentConversation?.id === conversationId) {
        const remaining = conversations.filter((c) => c.id !== conversationId);
        if (remaining.length > 0) {
          const conv = await fetchConversation(projectId, remaining[0].id);
          setCurrentConversation(conv);
          setMessages(conv.messages || []);
        } else {
          setCurrentConversation(null);
          setMessages([]);
        }
        setActivityItems([]);
      }
      toast.success('Conversation deleted');
    } catch (error) {
      console.error('Failed to delete conversation:', error);
      toast.error('Failed to delete conversation');
    }
  };

  // Send message
  const handleSendMessage = useCallback(async () => {
    if (!projectId || !input.trim() || isStreaming) return;

    const userMessage = input.trim();
    setInput('');
    setIsStreaming(true);
    setStreamingText('');
    setActivityItems([]);

    // Add user message to UI immediately
    const tempUserMessage: Message = {
      id: `temp-${Date.now()}`,
      conversation_id: currentConversation?.id || '',
      role: 'user',
      content: userMessage,
      timestamp: new Date().toISOString(),
      is_error: false,
    };
    setMessages((prev) => [...prev, tempUserMessage]);

    // Create abort controller
    abortControllerRef.current = new AbortController();

    try {
      let conversationId = currentConversation?.id;
      let fullText = '';

      await invokeAgent({
        projectId,
        conversationId,
        message: userMessage,
        clusterId: selectedClusterId,
        defaultCatalog,
        defaultSchema,
        signal: abortControllerRef.current.signal,
        onEvent: (event) => {
          const type = event.type as string;

          if (type === 'conversation.created') {
            conversationId = event.conversation_id as string;
            fetchConversations(projectId).then(setConversations);
          } else if (type === 'text') {
            const text = event.text as string;
            fullText += text;
            setStreamingText(fullText);
          } else if (type === 'thinking') {
            const thinking = event.thinking as string;
            setActivityItems((prev) => [
              ...prev,
              {
                id: `thinking-${Date.now()}`,
                type: 'thinking',
                content: thinking,
                timestamp: Date.now(),
              },
            ]);
          } else if (type === 'tool_use') {
            setActivityItems((prev) => [
              ...prev,
              {
                id: event.tool_id as string,
                type: 'tool_use',
                content: '',
                toolName: event.tool_name as string,
                toolInput: event.tool_input as Record<string, unknown>,
                timestamp: Date.now(),
              },
            ]);
          } else if (type === 'tool_result') {
            const content = event.content as string;
            setActivityItems((prev) => [
              ...prev,
              {
                id: `result-${event.tool_use_id}`,
                type: 'tool_result',
                content: typeof content === 'string' ? content : JSON.stringify(content),
                isError: event.is_error as boolean,
                timestamp: Date.now(),
              },
            ]);
          } else if (type === 'error') {
            toast.error(event.error as string);
          }
        },
        onError: (error) => {
          console.error('Stream error:', error);
          toast.error('Failed to get response');
        },
        onDone: async () => {
          if (fullText) {
            const assistantMessage: Message = {
              id: `msg-${Date.now()}`,
              conversation_id: conversationId || '',
              role: 'assistant',
              content: fullText,
              timestamp: new Date().toISOString(),
              is_error: false,
            };
            setMessages((prev) => [...prev, assistantMessage]);
          }
          setStreamingText('');
          setIsStreaming(false);

          if (conversationId && !currentConversation?.id) {
            const conv = await fetchConversation(projectId, conversationId);
            setCurrentConversation(conv);
          }
        },
      });
    } catch (error) {
      console.error('Failed to send message:', error);
      toast.error('Failed to send message');
      setIsStreaming(false);
    }
  }, [projectId, input, isStreaming, currentConversation?.id, selectedClusterId, defaultCatalog, defaultSchema]);

  // Handle keyboard submit
  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSendMessage();
    }
  };

  if (isLoading) {
    return (
      <MainLayout projectName={project?.name}>
        <div className="flex h-full items-center justify-center">
          <Loader2 className="h-8 w-8 animate-spin text-[var(--color-text-muted)]" />
        </div>
      </MainLayout>
    );
  }

  const sidebar = (
    <Sidebar
      conversations={conversations}
      currentConversationId={currentConversation?.id}
      onConversationSelect={handleSelectConversation}
      onNewConversation={handleNewConversation}
      onDeleteConversation={handleDeleteConversation}
      isLoading={false}
    />
  );

  return (
    <MainLayout projectName={project?.name} sidebar={sidebar}>
      <div className="flex flex-1 flex-col h-full">
        {/* Chat Header */}
        {currentConversation && (
          <div className="flex h-14 items-center justify-between border-b border-[var(--color-border)] px-6 bg-[var(--color-bg-secondary)]/50">
            <h2 className="font-medium text-[var(--color-text-heading)] truncate max-w-[200px]">
              {currentConversation.title}
            </h2>
            <div className="flex items-center gap-3">
              {/* Catalog Input */}
              <div className="flex items-center gap-1">
                <label className="text-xs text-[var(--color-text-muted)]">Catalog:</label>
                <input
                  type="text"
                  value={defaultCatalog}
                  onChange={(e) => setDefaultCatalog(e.target.value)}
                  placeholder="catalog"
                  className="h-8 w-28 px-2 rounded-md border border-[var(--color-border)] bg-[var(--color-background)] text-xs text-[var(--color-text-primary)] placeholder:text-[var(--color-text-muted)] focus:outline-none focus:ring-2 focus:ring-[var(--color-accent-primary)]/50"
                />
              </div>
              {/* Schema Input */}
              <div className="flex items-center gap-1">
                <label className="text-xs text-[var(--color-text-muted)]">Schema:</label>
                <input
                  type="text"
                  value={defaultSchema}
                  onChange={(e) => setDefaultSchema(e.target.value)}
                  placeholder="schema"
                  className="h-8 w-32 px-2 rounded-md border border-[var(--color-border)] bg-[var(--color-background)] text-xs text-[var(--color-text-primary)] placeholder:text-[var(--color-text-muted)] focus:outline-none focus:ring-2 focus:ring-[var(--color-accent-primary)]/50"
                />
              </div>
              {/* Open Catalog Button */}
              {workspaceUrl && defaultCatalog && defaultSchema && (
                <a
                  href={`${workspaceUrl}/explore/data/${defaultCatalog}/${defaultSchema}`}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="flex items-center justify-center h-8 w-8 rounded-md border border-[var(--color-border)] bg-[var(--color-background)] text-[var(--color-text-muted)] hover:bg-[var(--color-bg-secondary)] hover:text-[var(--color-text-primary)] focus:outline-none focus:ring-2 focus:ring-[var(--color-accent-primary)]/50 transition-colors"
                  title="Open in Catalog Explorer"
                >
                  <ExternalLink className="h-4 w-4" />
                </a>
              )}
              {/* Cluster Dropdown */}
              {clusters.length > 0 && (
              <div className="relative" ref={clusterDropdownRef}>
                <button
                  onClick={() => setClusterDropdownOpen(!clusterDropdownOpen)}
                  className="flex items-center gap-2 h-8 px-3 rounded-md border border-[var(--color-border)] bg-[var(--color-background)] text-xs text-[var(--color-text-primary)] hover:bg-[var(--color-bg-secondary)] focus:outline-none focus:ring-2 focus:ring-[var(--color-accent-primary)]/50 transition-colors"
                >
                  {(() => {
                    const selected = clusters.find(c => c.cluster_id === selectedClusterId);
                    return selected ? (
                      <>
                        <span className={cn(
                          'w-2 h-2 rounded-full',
                          selected.state === 'RUNNING' ? 'bg-green-500' : 'bg-gray-400'
                        )} />
                        <span className="max-w-[180px] truncate">{selected.cluster_name}</span>
                      </>
                    ) : (
                      <span className="text-[var(--color-text-muted)]">Loading...</span>
                    );
                  })()}
                  <ChevronDown className={cn('w-3 h-3 transition-transform', clusterDropdownOpen && 'rotate-180')} />
                </button>
                {clusterDropdownOpen && (
                  <div className="absolute right-0 top-full mt-1 w-72 max-h-64 overflow-y-auto rounded-md border border-[var(--color-border)] bg-[var(--color-background)] shadow-lg z-50">
                    {clusters.map((cluster) => (
                      <button
                        key={cluster.cluster_id}
                        onClick={() => {
                          setSelectedClusterId(cluster.cluster_id);
                          setClusterDropdownOpen(false);
                        }}
                        className={cn(
                          'w-full flex items-center gap-2 px-3 py-2 text-xs text-left hover:bg-[var(--color-bg-secondary)] transition-colors',
                          selectedClusterId === cluster.cluster_id && 'bg-[var(--color-bg-secondary)]'
                        )}
                      >
                        <span className={cn(
                          'w-2 h-2 rounded-full flex-shrink-0',
                          cluster.state === 'RUNNING' ? 'bg-green-500' : 'bg-gray-400'
                        )} />
                        <span className="truncate text-[var(--color-text-primary)]">{cluster.cluster_name}</span>
                      </button>
                    ))}
                  </div>
                )}
              </div>
              )}
            </div>
          </div>
        )}

        {/* Messages */}
        <div className="flex-1 overflow-y-auto p-6">
          {messages.length === 0 && !streamingText ? (
            <div className="flex h-full items-center justify-center">
              <div className="text-center">
                <MessageSquare className="mx-auto h-12 w-12 text-[var(--color-text-muted)]/40" />
                <h3 className="mt-4 text-lg font-medium text-[var(--color-text-heading)]">
                  Start a conversation
                </h3>
                <p className="mt-2 text-sm text-[var(--color-text-muted)]">
                  Ask Claude to help you with code in this project
                </p>
              </div>
            </div>
          ) : (
            <div className="mx-auto max-w-5xl space-y-4">
              {messages.map((message) => (
                <div
                  key={message.id}
                  className={cn(
                    'flex',
                    message.role === 'user' ? 'justify-end' : 'justify-start'
                  )}
                >
                  <div
                    className={cn(
                      'max-w-[85%] rounded-lg px-3 py-2 shadow-sm',
                      message.role === 'user'
                        ? 'bg-[var(--color-accent-primary)] text-white'
                        : 'bg-[var(--color-bg-secondary)] border border-[var(--color-border)]/50',
                      message.is_error && 'bg-[var(--color-error)]/10 border-[var(--color-error)]/30'
                    )}
                  >
                    {message.role === 'assistant' ? (
                      <div className="prose prose-xs max-w-none text-[var(--color-text-primary)] text-[13px] leading-relaxed">
                        <ReactMarkdown remarkPlugins={[remarkGfm]}>
                          {message.content}
                        </ReactMarkdown>
                      </div>
                    ) : (
                      <p className="whitespace-pre-wrap text-[13px]">{message.content}</p>
                    )}
                  </div>
                </div>
              ))}

              {/* Activity section (thinking, tools) */}
              {(activityItems.length > 0 || isStreaming) && (
                <ActivitySection items={activityItems} isStreaming={isStreaming && !streamingText} />
              )}

              {/* Streaming response */}
              {streamingText && (
                <div className="flex justify-start">
                  <div className="max-w-[85%] rounded-xl bg-[var(--color-bg-secondary)] border border-[var(--color-border)]/50 px-3 py-2 shadow-sm">
                    <div className="prose prose-xs max-w-none text-[var(--color-text-primary)] text-[13px] leading-relaxed">
                      <ReactMarkdown remarkPlugins={[remarkGfm]}>
                        {streamingText}
                      </ReactMarkdown>
                    </div>
                  </div>
                </div>
              )}

              {/* Loading indicator */}
              {isStreaming && !streamingText && activityItems.length === 0 && (
                <div className="flex justify-start">
                  <div className="rounded-xl bg-[var(--color-bg-secondary)] border border-[var(--color-border)]/50 px-4 py-3 shadow-sm">
                    <Loader2 className="h-5 w-5 animate-spin text-[var(--color-text-muted)]" />
                  </div>
                </div>
              )}

              <div ref={messagesEndRef} />
            </div>
          )}
        </div>

        {/* Input Area */}
        <div className="border-t border-[var(--color-border)] p-4 bg-[var(--color-bg-secondary)]/30">
          <div className="mx-auto max-w-5xl">
            <div className="flex gap-3">
              <textarea
                ref={inputRef}
                value={input}
                onChange={(e) => setInput(e.target.value)}
                onKeyDown={handleKeyDown}
                placeholder="Ask Claude to help with code..."
                rows={1}
                className="flex-1 resize-none rounded-xl border border-[var(--color-border)] bg-[var(--color-background)] px-4 py-3 text-sm text-[var(--color-text-primary)] placeholder:text-[var(--color-text-muted)] focus:outline-none focus:ring-2 focus:ring-[var(--color-accent-primary)]/50 focus:border-[var(--color-accent-primary)] disabled:cursor-not-allowed disabled:opacity-50 transition-all"
                disabled={isStreaming}
              />
              <Button
                onClick={handleSendMessage}
                disabled={!input.trim() || isStreaming}
                className="h-12 w-12 rounded-xl"
              >
                {isStreaming ? (
                  <Loader2 className="h-5 w-5 animate-spin" />
                ) : (
                  <Send className="h-5 w-5" />
                )}
              </Button>
            </div>
            <p className="mt-2 text-xs text-[var(--color-text-muted)]">
              Press Enter to send, Shift+Enter for new line
            </p>
          </div>
        </div>
      </div>
    </MainLayout>
  );
}
