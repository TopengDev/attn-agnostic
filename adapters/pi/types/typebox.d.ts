/**
 * Ambient declaration for the subset of `typebox` the adapter uses for tool
 * parameter schemas. pi provides `typebox` at runtime, so it is intentionally
 * NOT a package.json dependency; this shim only lets `tsc` type-check
 * `src/index.ts` offline. The real `typebox`/`@sinclair/typebox` types take
 * precedence if installed.
 */
declare module 'typebox' {
  /** Opaque schema value — pi/typebox interpret it at runtime. */
  export interface TSchema {
    readonly __typeboxSchema?: unique symbol;
  }

  interface SchemaOptions {
    description?: string;
    [key: string]: unknown;
  }

  export const Type: {
    Object(properties: Record<string, TSchema>, options?: SchemaOptions): TSchema;
    String(options?: SchemaOptions): TSchema;
    Number(options?: SchemaOptions): TSchema;
    Boolean(options?: SchemaOptions): TSchema;
    Optional(schema: TSchema): TSchema;
  };
}
