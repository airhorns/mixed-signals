import { t as Transport } from "./protocol.shared.js";
import { ModelConstructor, ModelFactory } from "@preact/signals-core";

//#region server/memory-transport.d.ts
/**
 * Creates two linked Transport instances for in-process communication.
 * Messages sent on one end are delivered to the other via queueMicrotask.
 */
declare function createMemoryTransportPair(): [Transport, Transport];
//#endregion
//#region shared/disposable.d.ts
declare global {
  interface SymbolConstructor {
    readonly dispose: unique symbol;
  }
  interface Disposable {
    [Symbol.dispose](): void;
  }
}
//#endregion
//#region server/model.d.ts
declare function createModel<TModel, TFactoryArgs extends any[] = []>(factory: ModelFactory<TModel, TFactoryArgs>): ModelConstructor<TModel, TFactoryArgs>;
//#endregion
//#region server/instances.d.ts
declare class Instances {
  private registry;
  private reverseRegistry;
  private nextIdCounter;
  nextId(): string;
  register(id: string, instance: any): void;
  get(id: string): any;
  getId(instance: any): string | undefined;
  remove(id: string): void;
}
//#endregion
//#region server/rpc.d.ts
type ModelConstructor$1 = (new (...args: any[]) => any) | ((...args: any[]) => any);
declare class RPC {
  private reflection;
  private clients;
  private root;
  /** @internal */
  instances: Instances;
  /** Registered upstream connections for model forwarding. */
  private upstreams;
  private nextUpstreamPrefix;
  constructor(root?: any);
  registerModel(name: string, Ctor: ModelConstructor$1): void;
  expose(root: any): void;
  /**
   * Register an upstream mixed-signals connection whose models are forwarded
   * to downstream clients. All models from the upstream are automatically
   * forwarded — no per-model declaration needed.
   */
  addUpstream(transport: Transport): () => void;
  addClient(transport: Transport, clientId?: string): () => void;
  /**
   * Called by ForwardedUpstream when the upstream root changes.
   * Sends the merged root to all clients once every upstream has reported.
   * @internal
   */
  onUpstreamRootChanged(): void;
  private allUpstreamsReady;
  private broadcastMergedRoot;
  /**
   * Merge local root with upstream roots and send to a client.
   */
  private sendMergedRoot;
  /**
   * Intercept a client message and forward it to an upstream if it targets
   * forwarded models/signals. Returns true if the message was forwarded.
   */
  private tryForwardClientMessage;
  private findUpstreamForSignal;
  private findUpstreamForInstance;
  private handleMessage;
  private callMethod;
  notify(method: string, params: any[], clientId?: string): void;
  /** @internal */
  send(clientId: string, message: string): void;
  private sendResult;
  private sendError;
}
//#endregion
export { RPC, type Transport, createMemoryTransportPair, createModel };