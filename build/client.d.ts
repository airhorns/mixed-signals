import { t as Transport } from "./protocol.shared.js";
import * as _preact_signals_core0 from "@preact/signals-core";
import { Signal } from "@preact/signals-core";

//#region client/rpc.d.ts
declare class RPCClient {
  private transport;
  private nextId;
  private pending;
  private notificationListeners;
  private localRoot;
  /** @internal */
  reflection: ClientReflection;
  private transportReady;
  root: any;
  ready: Promise<void>;
  private _resolveReady;
  constructor(transport: Transport, ctx?: any);
  /**
   * Replace the transport and reset internal state for a reconnection.
   * A new `ready` promise is created that resolves on the next `@R` message.
   */
  reconnect(transport: Transport): void;
  private wireTransport;
  registerModel(typeName: string, ctor: any): void;
  /**
   * Publish an object as the dispatch target for peer-issued method
   * calls. Mirrors the server's `RPC.expose`: an inbound `M{id}:method`
   * frame is dispatched against this root using the same dot-notation
   * lookup the server uses for nested methods (e.g. `"browser.logs"`
   * walks `root.browser.logs`). Returning a non-promise sends `R{id}`
   * with the value; throwing or rejecting sends `E{id}` with the
   * `{code, message}` shape. Calling `expose` again replaces the prior
   * root.
   */
  expose(root: any): void;
  private handleCall;
  call(method: string, params?: any): Promise<any>;
  notify(method: string, params?: any[]): void;
  private sendCall;
  onNotification(cb: (method: string, params: any[]) => void): () => void;
  private handleNotification;
}
//#endregion
//#region client/reflection.d.ts
/** @internal */
interface WireContext {
  rpc: RPCClient;
}
declare class ClientReflection {
  private signals;
  private models;
  private modelRegistry;
  private rpc;
  private ctx;
  private watchBatch;
  private unwatchBatch;
  private watchFlushTimer;
  private unwatchFlushTimer;
  constructor(rpc: RPCClient, ctx?: any);
  /** Clear cached signals and model facades so a reconnection gets fresh state. */
  reset(): void;
  registerModel(typeName: string, ctor: any): void;
  private scheduleWatch;
  private scheduleUnwatch;
  getOrCreateSignal(id: number | string, initialValue: any): Signal<any>;
  createModelFacade(serialized: any): any;
  handleUpdate(id: number | string, value: any, mode?: string): void;
}
//#endregion
//#region client/model.d.ts
declare function createReflectedModel<T>(signalProps: string[], methods: string[]): _preact_signals_core0.ModelConstructor<T, [ctx: WireContext, data: any]>;
//#endregion
export { RPCClient, type Transport, createReflectedModel };