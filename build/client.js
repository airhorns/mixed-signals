import { a as formatCallMessage, c as formatResultMessage, d as parseWireValue, i as WATCH_SIGNALS_METHOD, l as parseWireMessage, n as SIGNAL_UPDATE_METHOD, o as formatErrorMessage, r as UNWATCH_SIGNALS_METHOD, s as formatNotificationMessage, t as ROOT_NOTIFICATION_METHOD, u as parseWireParams } from "./protocol.shared.js";
import { Signal, computed, createModel, signal } from "@preact/signals-core";

//#region client/model.ts
function createReflectedModel(signalProps, methods) {
	return createModel((ctx, data) => {
		const model = {};
		const wireId = data["@wireId"];
		model.id = signal(wireId);
		for (const prop of signalProps) if (data?.[prop] instanceof Signal) model[prop] = computed(() => data[prop].value);
		for (const method of methods) model[method] = async (...args) => {
			return ctx.rpc.call(`${wireId}#${method}`, args);
		};
		return model;
	});
}

//#endregion
//#region client/reflection.ts
var ClientReflection = class {
	signals = /* @__PURE__ */ new Map();
	models = /* @__PURE__ */ new Map();
	modelRegistry = /* @__PURE__ */ new Map();
	rpc;
	ctx;
	watchBatch = /* @__PURE__ */ new Set();
	unwatchBatch = /* @__PURE__ */ new Set();
	watchFlushTimer = null;
	unwatchFlushTimer = null;
	constructor(rpc, ctx) {
		this.rpc = rpc;
		this.ctx = ctx && ctx.rpc === rpc ? ctx : { rpc };
	}
	/** Clear cached signals and model facades so a reconnection gets fresh state. */
	reset() {
		this.signals.clear();
		this.models.clear();
		this.watchBatch.clear();
		this.unwatchBatch.clear();
		if (this.watchFlushTimer) {
			clearTimeout(this.watchFlushTimer);
			this.watchFlushTimer = null;
		}
		if (this.unwatchFlushTimer) {
			clearTimeout(this.unwatchFlushTimer);
			this.unwatchFlushTimer = null;
		}
	}
	registerModel(typeName, ctor) {
		this.modelRegistry.set(typeName, ctor);
	}
	scheduleWatch(id) {
		this.watchBatch.add(id);
		if (!this.watchFlushTimer) this.watchFlushTimer = setTimeout(() => {
			const ids = Array.from(this.watchBatch);
			this.watchBatch.clear();
			this.watchFlushTimer = null;
			if (ids.length > 0) this.rpc.notify(WATCH_SIGNALS_METHOD, ids);
		}, 1);
	}
	scheduleUnwatch(id) {
		this.unwatchBatch.add(id);
		if (!this.unwatchFlushTimer) this.unwatchFlushTimer = setTimeout(() => {
			const ids = Array.from(this.unwatchBatch);
			this.unwatchBatch.clear();
			this.unwatchFlushTimer = null;
			if (ids.length > 0) this.rpc.notify(UNWATCH_SIGNALS_METHOD, ids);
		}, 1);
	}
	getOrCreateSignal(id, initialValue) {
		const existingSignal = this.signals.get(id);
		if (existingSignal) return existingSignal;
		let unwatchTimeout = null;
		const createdSignal = signal(initialValue, {
			watched: () => {
				if (unwatchTimeout) {
					clearTimeout(unwatchTimeout);
					unwatchTimeout = null;
				} else this.scheduleWatch(id);
			},
			unwatched: () => {
				unwatchTimeout = setTimeout(() => {
					this.scheduleUnwatch(id);
					unwatchTimeout = null;
				}, 10);
			}
		});
		this.signals.set(id, createdSignal);
		return createdSignal;
	}
	createModelFacade(serialized) {
		const raw = serialized["@M"];
		if (!raw) throw new Error("Model missing @M field");
		const existing = this.models.get(raw);
		if (existing) return existing;
		const hashIdx = raw.lastIndexOf("#");
		const typeName = hashIdx !== -1 ? raw.slice(0, hashIdx) : raw;
		const wireId = hashIdx !== -1 ? raw.slice(hashIdx + 1) : void 0;
		const ModelCtor = this.modelRegistry.get(typeName);
		if (!ModelCtor) throw new Error(`Unknown model type: ${typeName}`);
		const model = new ModelCtor(this.ctx, {
			...serialized,
			"@wireId": wireId
		});
		this.models.set(raw, model);
		return model;
	}
	handleUpdate(id, value, mode) {
		const sig = this.signals.get(id);
		if (!sig) return;
		if (!mode) {
			sig.value = value;
			return;
		}
		const current = sig.value;
		switch (mode) {
			case "append":
				if (Array.isArray(current)) sig.value = [...current, ...value];
				else if (typeof current === "string") sig.value = current + value;
				break;
			case "merge":
				if (current && typeof current === "object") sig.value = {
					...current,
					...value
				};
				break;
			case "splice":
				if (Array.isArray(current)) {
					const { start, deleteCount, items } = value;
					const nextArray = [...current];
					nextArray.splice(start, deleteCount, ...items);
					sig.value = nextArray;
				}
				break;
			default: sig.value = value;
		}
	}
};

//#endregion
//#region client/rpc.ts
function dlv(obj, path) {
	return path.split(".").reduce((acc, key) => acc?.[key], obj);
}
var RPCClient = class {
	transport;
	nextId = 1;
	pending = /* @__PURE__ */ new Map();
	notificationListeners = /* @__PURE__ */ new Set();
	localRoot;
	/** @internal */
	reflection;
	transportReady;
	root = void 0;
	ready;
	_resolveReady;
	constructor(transport, ctx) {
		this.transport = transport;
		this.transportReady = transport.ready;
		this.ready = new Promise((resolve) => {
			this._resolveReady = resolve;
		});
		this.reflection = new ClientReflection(this, ctx);
		this.wireTransport(transport);
	}
	/**
	* Replace the transport and reset internal state for a reconnection.
	* A new `ready` promise is created that resolves on the next `@R` message.
	*/
	reconnect(transport) {
		this.transport = transport;
		this.transportReady = transport.ready;
		for (const { reject } of this.pending.values()) reject(/* @__PURE__ */ new Error("Transport reconnected"));
		this.pending.clear();
		this.reflection.reset();
		this.ready = new Promise((resolve) => {
			this._resolveReady = resolve;
		});
		this.wireTransport(transport);
	}
	wireTransport(transport) {
		transport.onMessage((data) => {
			const message = parseWireMessage(data.toString());
			if (!message) return;
			const reviver = (_key, val) => {
				if (typeof val === "object" && val) {
					if ("@S" in val) return this.reflection.getOrCreateSignal(val["@S"], val.v);
					if ("@M" in val) return this.reflection.createModelFacade(val);
				}
				return val;
			};
			if (message.type === "result" || message.type === "error") {
				const parsed = parseWireValue(message.payload, reviver);
				const pending = this.pending.get(message.id);
				if (!pending) return;
				this.pending.delete(message.id);
				if (message.type === "result") {
					pending.resolve(parsed);
					return;
				}
				pending.reject(new Error(parsed.message));
				return;
			}
			if (message.type === "call") {
				this.handleCall(message.id, message.method, message.payload, reviver);
				return;
			}
			const params = parseWireParams(message.payload, reviver);
			this.handleNotification(message.method, params);
		});
	}
	registerModel(typeName, ctor) {
		this.reflection.registerModel(typeName, ctor);
	}
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
	expose(root) {
		this.localRoot = root;
	}
	handleCall(id, method, payload, reviver) {
		const segments = method.split(".");
		const methodName = segments.pop();
		const receiver = segments.length > 0 ? dlv(this.localRoot, segments.join(".")) : this.localRoot;
		const target = receiver?.[methodName];
		if (typeof target !== "function") {
			this.transport.send(formatErrorMessage(id, {
				code: -1,
				message: `Method not found: ${method}`
			}));
			return;
		}
		let params;
		try {
			params = parseWireParams(payload, reviver);
		} catch (error) {
			this.transport.send(formatErrorMessage(id, {
				code: -1,
				message: error?.message ?? String(error)
			}));
			return;
		}
		Promise.resolve().then(() => target.apply(receiver, params)).then((result) => this.transport.send(formatResultMessage(id, result)), (error) => this.transport.send(formatErrorMessage(id, {
			code: -1,
			message: error?.message ?? String(error)
		})));
	}
	async call(method, params) {
		if (this.transportReady) await this.transportReady;
		return new Promise((resolve, reject) => {
			this.sendCall(method, params, resolve, reject);
		});
	}
	notify(method, params) {
		const message = formatNotificationMessage(method, params);
		if (this.transportReady) this.transportReady.then(() => this.transport.send(message));
		else this.transport.send(message);
	}
	sendCall(method, params, resolve, reject) {
		const id = this.nextId++;
		this.pending.set(id, {
			resolve,
			reject
		});
		this.transport.send(formatCallMessage(id, method, params || []));
	}
	onNotification(cb) {
		this.notificationListeners.add(cb);
		return () => {
			this.notificationListeners.delete(cb);
		};
	}
	handleNotification(method, params) {
		if (method === ROOT_NOTIFICATION_METHOD) {
			this.root = params[0];
			this._resolveReady();
		} else if (method === SIGNAL_UPDATE_METHOD) {
			const [id, value, mode] = params;
			this.reflection.handleUpdate(id, value, mode);
		} else for (const listener of this.notificationListeners) listener(method, params);
	}
};

//#endregion
export { RPCClient, createReflectedModel };