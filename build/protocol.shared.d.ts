//#region shared/protocol.d.ts
interface Transport {
  send(data: string): void;
  onMessage(cb: (data: {
    toString(): string;
  }) => void): void;
  ready?: Promise<void>;
}
//#endregion
export { Transport as t };