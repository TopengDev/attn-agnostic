/**
 * Ambient declaration for the subset of pi's extension API the adapter uses.
 *
 * pi (`earendil-works/pi`, MIT, Node >=18 — NOT Inflection's Pi) provides
 * `@earendil-works/pi-coding-agent` at runtime when it loads the extension, so
 * it is intentionally NOT a dependency in package.json. This shim exists only so
 * `tsc` can type-check `src/index.ts` offline, against the exact surface we
 * depend on. If pi's real types are installed they take precedence; keep this in
 * sync with the version pinned in README.md.
 */
declare module '@earendil-works/pi-coding-agent' {
  export type DeliverAs = 'steer' | 'followUp';

  export interface ToolContent {
    type: 'text';
    text: string;
  }

  export interface ToolResult {
    content: ToolContent[];
    details?: unknown;
  }

  export interface ToolDefinition {
    name: string;
    label?: string;
    description?: string;
    /** A typebox schema (Type.Object(...)). Typed loosely — pi validates at runtime. */
    parameters: unknown;
    execute(toolCallId: string, params: any): Promise<ToolResult> | ToolResult;
  }

  export type SessionEvent = 'session_start' | 'session_shutdown';

  export interface ExtensionAPI {
    /** Inject a message into the live session turn. */
    sendUserMessage(text: string, opts?: { deliverAs?: DeliverAs }): unknown;
    /** Subscribe to a session lifecycle event. */
    on(event: SessionEvent, listener: () => void | Promise<void>): void;
    /** Register a tool the model can call. */
    registerTool(def: ToolDefinition): void;
  }
}
