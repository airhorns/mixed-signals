import { a as formatCallMessage, c as formatResultMessage, i as WATCH_SIGNALS_METHOD, l as parseWireMessage, n as SIGNAL_UPDATE_METHOD, o as formatErrorMessage, r as UNWATCH_SIGNALS_METHOD, s as formatNotificationMessage, t as ROOT_NOTIFICATION_METHOD, u as parseWireParams } from "./protocol.shared.js";
import { Signal, createModel as createModel$1 } from "@preact/signals-core";

//#region server/memory-transport.ts
/**
* Creates two linked Transport instances for in-process communication.
* Messages sent on one end are delivered to the other via queueMicrotask.
*/
function createMemoryTransportPair() {
	let handlerA;
	let handlerB;
	return [{
		send(data) {
			queueMicrotask(() => handlerB?.({ toString: () => data }));
		},
		onMessage(cb) {
			handlerA = cb;
		},
		ready: Promise.resolve()
	}, {
		send(data) {
			queueMicrotask(() => handlerA?.({ toString: () => data }));
		},
		onMessage(cb) {
			handlerB = cb;
		},
		ready: Promise.resolve()
	}];
}

//#endregion
//#region server/model.ts
function createModel(factory) {
	const Ctor = createModel$1(((...args) => {
		const model = factory(...args);
		Object.setPrototypeOf(model, Ctor.prototype);
		return model;
	}));
	return Ctor;
}

//#endregion
//#region server/forwarding.ts
const SEP = "_";
/**
* Recursively adds an upstream prefix to all @S and @M markers in a parsed JSON value.
* Uses "_" as the separator to avoid colliding with the wire format's ":" field separator.
*/
function addPrefix(prefix, value) {
	if (value === null || value === void 0 || typeof value !== "object") return value;
	if (Array.isArray(value)) return value.map((v) => addPrefix(prefix, v));
	const out = {};
	for (const key of Object.keys(value)) {
		const v = value[key];
		if (key === "@S" && typeof v === "number") out["@S"] = `${prefix}${SEP}${v}`;
		else if (key === "@M" && typeof v === "string") {
			const h = v.lastIndexOf("#");
			out["@M"] = h === -1 ? v : `${v.slice(0, h + 1)}${prefix}${SEP}${v.slice(h + 1)}`;
		} else out[key] = addPrefix(prefix, v);
	}
	return out;
}
/**
* Check if a signal ID or instance ID belongs to an upstream with the given prefix.
*/
function isUpstreamId(prefix, id) {
	return typeof id === "string" && id.startsWith(`${prefix}${SEP}`);
}
/**
* Strip the prefix from a prefixed signal ID, returning the original numeric ID.
*/
function stripSignalPrefix(prefix, id) {
	return Number(id.slice(prefix.length + 1));
}
/**
* Strip the prefix from a prefixed instance ID, returning the original ID.
*/
function stripInstancePrefix(prefix, id) {
	return id.slice(prefix.length + 1);
}
/**
* Quick check: does the raw payload string contain any @S or @M markers
* that would require JSON rewriting? Avoids parsing for simple streaming deltas.
*/
function needsRewrite(rawPayload) {
	return rawPayload.includes("\"@S\"") || rawPayload.includes("\"@M\"");
}
/**
* Manages a single upstream connection. Intercepts wire messages from the
* upstream and rewrites IDs before forwarding to downstream clients.
*/
var ForwardedUpstream = class {
	prefix;
	transport;
	host;
	disposed = false;
	/** Rewritten root from upstream, ready for merging into downstream root. */
	root = void 0;
	/** Resolves when the upstream root has been received. */
	ready;
	_resolveReady;
	/** Upstream call ID → { clientId, downstreamCallId } */
	pendingCalls = /* @__PURE__ */ new Map();
	nextUpstreamCallId = 1;
	/** The single downstream client ID using this upstream (1:1 per-browser mapping). */
	clientId;
	constructor(prefix, transport, host) {
		this.prefix = prefix;
		this.transport = transport;
		this.host = host;
		this.ready = new Promise((resolve) => {
			this._resolveReady = resolve;
		});
		transport.onMessage((data) => {
			this.handleUpstreamMessage(data.toString());
		});
	}
	setClient(clientId) {
		this.clientId = clientId;
	}
	handleUpstreamMessage(msg) {
		if (this.disposed) return;
		const parsed = parseWireMessage(msg);
		if (!parsed) return;
		if (parsed.type === "notification") {
			if (parsed.method === ROOT_NOTIFICATION_METHOD) {
				const [rootValue] = parseWireParams(parsed.payload);
				this.root = addPrefix(this.prefix, rootValue);
				this._resolveReady();
				this.host.onUpstreamRootChanged();
				return;
			}
			if (parsed.method === SIGNAL_UPDATE_METHOD) {
				if (!this.clientId) return;
				const [signalId, value, mode] = parseWireParams(parsed.payload);
				const prefixedId = `${this.prefix}${SEP}${signalId}`;
				const rewrittenValue = needsRewrite(parsed.payload) ? addPrefix(this.prefix, value) : value;
				const outParams = mode ? [
					prefixedId,
					rewrittenValue,
					mode
				] : [prefixedId, rewrittenValue];
				this.host.send(this.clientId, formatNotificationMessage(SIGNAL_UPDATE_METHOD, outParams));
				return;
			}
		}
		if (parsed.type === "result") {
			const pending = this.pendingCalls.get(parsed.id);
			if (!pending) return;
			this.pendingCalls.delete(parsed.id);
			const result = JSON.parse(parsed.payload);
			const rewritten = needsRewrite(parsed.payload) ? addPrefix(this.prefix, result) : result;
			this.host.send(pending.clientId, formatResultMessage(pending.callId, rewritten));
			return;
		}
		if (parsed.type === "error") {
			const pending = this.pendingCalls.get(parsed.id);
			if (!pending) return;
			this.pendingCalls.delete(parsed.id);
			this.host.send(pending.clientId, formatErrorMessage(pending.callId, JSON.parse(parsed.payload)));
		}
	}
	/**
	* Forward a method call from a downstream client to the upstream.
	*/
	forwardCall(clientId, downstreamCallId, method, rawPayload) {
		const upstreamCallId = this.nextUpstreamCallId++;
		this.pendingCalls.set(upstreamCallId, {
			clientId,
			callId: downstreamCallId
		});
		const params = parseWireParams(rawPayload);
		this.transport.send(formatCallMessage(upstreamCallId, method, params));
	}
	/**
	* Forward watch requests to the upstream.
	*/
	forwardWatch(signalIds) {
		this.transport.send(formatNotificationMessage(WATCH_SIGNALS_METHOD, signalIds));
	}
	/**
	* Forward unwatch requests to the upstream.
	*/
	forwardUnwatch(signalIds) {
		this.transport.send(formatNotificationMessage(UNWATCH_SIGNALS_METHOD, signalIds));
	}
	/**
	* Clear the association with a downstream client (client disconnected).
	*/
	removeClient(clientId) {
		if (this.clientId === clientId) {
			this.clientId = void 0;
			this.pendingCalls.clear();
		}
	}
	/**
	* Tear down this upstream connection entirely.
	*/
	dispose() {
		this.disposed = true;
		this.clientId = void 0;
		this.pendingCalls.clear();
	}
};

//#endregion
//#region server/instances.ts
var Instances = class {
	registry = /* @__PURE__ */ new Map();
	reverseRegistry = /* @__PURE__ */ new WeakMap();
	nextIdCounter = 1;
	nextId() {
		while (this.registry.has(String(this.nextIdCounter))) this.nextIdCounter++;
		return String(this.nextIdCounter++);
	}
	register(id, instance) {
		this.registry.set(id, instance);
		if (typeof instance === "object" && instance !== null) this.reverseRegistry.set(instance, id);
	}
	get(id) {
		return this.registry.get(id);
	}
	getId(instance) {
		if (typeof instance !== "object" || instance === null) return void 0;
		return this.reverseRegistry.get(instance);
	}
	remove(id) {
		const instance = this.registry.get(id);
		if (instance && typeof instance === "object") this.reverseRegistry.delete(instance);
		this.registry.delete(id);
	}
};

//#endregion
//#region server/reflection.ts
var Reflection = class {
	signalIds = /* @__PURE__ */ new WeakMap();
	signals = /* @__PURE__ */ new Map();
	subscriptions = /* @__PURE__ */ new Map();
	lastSentValues = /* @__PURE__ */ new Map();
	sentModels = /* @__PURE__ */ new Map();
	nextSignalId = 1;
	rpc;
	instances;
	modelRegistry = /* @__PURE__ */ new Map();
	autoIds = /* @__PURE__ */ new WeakMap();
	constructor(rpc, instances) {
		this.rpc = rpc;
		this.instances = instances;
	}
	registerModel(name, Ctor) {
		this.modelRegistry.set(Ctor, name);
	}
	isModel(val) {
		if (typeof val !== "object" || val === null) return false;
		for (const Ctor of this.modelRegistry.keys()) if (val instanceof Ctor) return true;
		return false;
	}
	getModelType(val) {
		for (const [Ctor, name] of this.modelRegistry) if (val instanceof Ctor) return name;
	}
	getInstanceId(instance) {
		const existingId = this.instances.getId(instance);
		if (existingId !== void 0) return existingId;
		if ("id" in instance) {
			const id = instance.id;
			return String(id instanceof Signal ? id.peek() : id);
		}
		let id = this.autoIds.get(instance);
		if (id === void 0) {
			id = this.instances.nextId();
			this.autoIds.set(instance, id);
		}
		return id;
	}
	getSignalId(sig) {
		let id = this.signalIds.get(sig);
		if (!id) {
			id = this.nextSignalId++;
			this.signalIds.set(sig, id);
			this.signals.set(id, sig);
		}
		return id;
	}
	serializeValue(value, clientId) {
		if (value === this.rpc || value === this || value === this.instances) return void 0;
		if (typeof value === "function") return void 0;
		if (value instanceof Signal) {
			const id = this.getSignalId(value);
			const signalValue = value.peek();
			if (clientId) {
				this.lastSentValues.set(`${clientId}:${id}`, signalValue);
				this.watch(clientId, id);
			}
			return {
				"@S": id,
				v: this.serializeValue(signalValue, clientId)
			};
		}
		if (this.isModel(value)) {
			const typeName = this.getModelType(value);
			const instanceId = this.getInstanceId(value);
			const marker = `${typeName}#${instanceId}`;
			if (!this.instances.get(instanceId)) this.instances.register(instanceId, value);
			if (clientId) {
				let sent = this.sentModels.get(clientId);
				if (sent?.has(marker)) return { "@M": marker };
				if (!sent) {
					sent = /* @__PURE__ */ new Set();
					this.sentModels.set(clientId, sent);
				}
				sent.add(marker);
			}
			const branded = { "@M": marker };
			for (const [key, prop] of Object.entries(value)) {
				if (key.startsWith("_")) continue;
				const serializedProp = this.serializeValue(prop, clientId);
				if (serializedProp !== void 0) branded[key] = serializedProp;
			}
			return branded;
		}
		if (Array.isArray(value)) return value.map((item) => {
			const serializedItem = this.serializeValue(item, clientId);
			return serializedItem === void 0 ? null : serializedItem;
		});
		if (value && typeof value === "object") {
			const serialized = {};
			for (const [key, prop] of Object.entries(value)) {
				if (key.startsWith("_")) continue;
				const serializedProp = this.serializeValue(prop, clientId);
				if (serializedProp !== void 0) serialized[key] = serializedProp;
			}
			return serialized;
		}
		return value;
	}
	serialize(value, clientId) {
		const serialized = this.serializeValue(value, clientId);
		if (serialized === void 0) return null;
		return JSON.parse(JSON.stringify(serialized));
	}
	watch(clientId, signalId) {
		let subs = this.subscriptions.get(signalId);
		if (!subs) {
			subs = /* @__PURE__ */ new Set();
			this.subscriptions.set(signalId, subs);
		}
		const isFirst = subs.size === 0;
		subs.add(clientId);
		if (isFirst) {
			const sig = this.signals.get(signalId);
			if (sig) sig.subscribe(() => {
				this.notifySubscribers(signalId);
			});
		}
	}
	unwatch(clientId, signalId) {
		this.subscriptions.get(signalId)?.delete(clientId);
	}
	removeClient(clientId) {
		for (const subs of this.subscriptions.values()) subs.delete(clientId);
		const prefix = `${clientId}:`;
		for (const key of this.lastSentValues.keys()) if (key.startsWith(prefix)) this.lastSentValues.delete(key);
		this.sentModels.delete(clientId);
	}
	notifySubscribers(signalId) {
		const sig = this.signals.get(signalId);
		const clients = this.subscriptions.get(signalId);
		if (!sig || !clients || clients.size === 0) return;
		const newValue = sig.peek();
		for (const clientId of clients) {
			const lastValue = this.lastSentValues.get(`${clientId}:${signalId}`);
			if (lastValue === newValue) continue;
			const update = this.computeDelta(lastValue, newValue);
			if (!update) continue;
			const serializedValue = this.serialize(update.value, clientId);
			const params = update.mode ? [
				signalId,
				serializedValue,
				update.mode
			] : [signalId, serializedValue];
			this.rpc.send(clientId, formatNotificationMessage(SIGNAL_UPDATE_METHOD, params));
			this.lastSentValues.set(`${clientId}:${signalId}`, newValue);
		}
	}
	/**
	* Compute the delta between the last-sent value and the new value.
	* Returns null if the values are shallow-equal (no update needed).
	*/
	computeDelta(oldValue, newValue) {
		if (oldValue === void 0) return { value: newValue };
		if (Array.isArray(oldValue) && Array.isArray(newValue)) {
			if (newValue.length > oldValue.length && oldValue.every((value, index) => value === newValue[index])) return {
				value: newValue.slice(oldValue.length),
				mode: "append"
			};
			if (newValue.length === oldValue.length && oldValue.every((value, index) => value === newValue[index])) return null;
		}
		if (oldValue && newValue && typeof oldValue === "object" && typeof newValue === "object" && !Array.isArray(oldValue)) {
			const changes = {};
			let hasChanges = false;
			for (const key in newValue) if (newValue[key] !== oldValue[key]) {
				changes[key] = newValue[key];
				hasChanges = true;
			}
			if (!hasChanges) {
				const oldKeys = Object.keys(oldValue);
				const newKeys = Object.keys(newValue);
				if (oldKeys.length === newKeys.length) return null;
			}
			if (hasChanges) return {
				value: changes,
				mode: "merge"
			};
		}
		if (typeof oldValue === "string" && typeof newValue === "string" && newValue.startsWith(oldValue)) {
			if (newValue.length === oldValue.length) return null;
			return {
				value: newValue.slice(oldValue.length),
				mode: "append"
			};
		}
		return { value: newValue };
	}
};

//#endregion
//#region server/rpc.ts
function dlv(obj, path) {
	return path.split(".").reduce((acc, key) => acc?.[key], obj);
}
var RPC = class {
	reflection;
	clients = /* @__PURE__ */ new Map();
	root;
	/** @internal */
	instances;
	/** Registered upstream connections for model forwarding. */
	upstreams = /* @__PURE__ */ new Map();
	nextUpstreamPrefix = 1;
	constructor(root) {
		this.instances = new Instances();
		this.reflection = new Reflection(this, this.instances);
		if (root !== void 0) this.expose(root);
	}
	registerModel(name, Ctor) {
		this.reflection.registerModel(name, Ctor);
	}
	expose(root) {
		this.root = root;
		this.instances.register("0", root);
	}
	/**
	* Register an upstream mixed-signals connection whose models are forwarded
	* to downstream clients. All models from the upstream are automatically
	* forwarded — no per-model declaration needed.
	*/
	addUpstream(transport) {
		const prefix = String(this.nextUpstreamPrefix++);
		const upstream = new ForwardedUpstream(prefix, transport, this);
		this.upstreams.set(prefix, upstream);
		for (const clientId of this.clients.keys()) upstream.setClient(clientId);
		return () => {
			upstream.dispose();
			this.upstreams.delete(prefix);
		};
	}
	addClient(transport, clientId) {
		const id = clientId ?? crypto.randomUUID();
		this.clients.set(id, transport);
		for (const upstream of this.upstreams.values()) upstream.setClient(id);
		transport.onMessage(async (data) => {
			try {
				const raw = data.toString();
				if (this.tryForwardClientMessage(id, raw)) return;
				const message = parseWireMessage(raw);
				if (!message || message.type === "result" || message.type === "error") return;
				const params = parseWireParams(message.payload);
				const messageId = message.type === "call" ? message.id : void 0;
				await this.handleMessage(id, messageId, message.method, params);
			} catch (err) {
				console.error("Failed to handle message:", err);
			}
		});
		if (this.allUpstreamsReady()) this.broadcastMergedRoot(id);
		return () => {
			this.clients.delete(id);
			this.reflection.removeClient(id);
			for (const upstream of this.upstreams.values()) upstream.removeClient(id);
		};
	}
	/**
	* Called by ForwardedUpstream when the upstream root changes.
	* Sends the merged root to all clients once every upstream has reported.
	* @internal
	*/
	onUpstreamRootChanged() {
		if (!this.allUpstreamsReady()) return;
		for (const clientId of this.clients.keys()) this.broadcastMergedRoot(clientId);
	}
	allUpstreamsReady() {
		for (const upstream of this.upstreams.values()) if (upstream.root === void 0) return false;
		return true;
	}
	broadcastMergedRoot(clientId) {
		const localRoot = this.root !== void 0 ? this.reflection.serialize(this.root, clientId) : void 0;
		this.sendMergedRoot(clientId, localRoot);
	}
	/**
	* Merge local root with upstream roots and send to a client.
	*/
	sendMergedRoot(clientId, localRoot) {
		let merged = localRoot;
		for (const upstream of this.upstreams.values()) if (upstream.root) if (merged && typeof merged === "object" && !Array.isArray(merged)) merged = {
			...merged,
			...upstream.root
		};
		else merged = upstream.root;
		if (merged !== void 0) this.send(clientId, formatNotificationMessage(ROOT_NOTIFICATION_METHOD, [merged]));
	}
	/**
	* Intercept a client message and forward it to an upstream if it targets
	* forwarded models/signals. Returns true if the message was forwarded.
	*/
	tryForwardClientMessage(clientId, raw) {
		const parsed = parseWireMessage(raw);
		if (!parsed) return false;
		if (parsed.type === "notification" && (parsed.method === WATCH_SIGNALS_METHOD || parsed.method === UNWATCH_SIGNALS_METHOD)) {
			const ids = parseWireParams(parsed.payload);
			const localIds = [];
			const upstreamBatches = /* @__PURE__ */ new Map();
			for (const id of ids) {
				const upstream = this.findUpstreamForSignal(id);
				if (upstream) {
					let batch = upstreamBatches.get(upstream);
					if (!batch) {
						batch = [];
						upstreamBatches.set(upstream, batch);
					}
					batch.push(stripSignalPrefix(upstream.prefix, id));
				} else localIds.push(id);
			}
			if (upstreamBatches.size === 0) return false;
			for (const [upstream, signalIds] of upstreamBatches) if (parsed.method === WATCH_SIGNALS_METHOD) upstream.forwardWatch(signalIds);
			else upstream.forwardUnwatch(signalIds);
			for (const signalId of localIds) if (parsed.method === WATCH_SIGNALS_METHOD) this.reflection.watch(clientId, signalId);
			else this.reflection.unwatch(clientId, signalId);
			return true;
		}
		if (parsed.type === "call") {
			const hashIdx = parsed.method.indexOf("#");
			if (hashIdx !== -1) {
				const wireId = parsed.method.slice(0, hashIdx);
				const upstream = this.findUpstreamForInstance(wireId);
				if (upstream) {
					const strippedWireId = stripInstancePrefix(upstream.prefix, wireId);
					const methodName = parsed.method.slice(hashIdx + 1);
					upstream.forwardCall(clientId, parsed.id, `${strippedWireId}#${methodName}`, parsed.payload);
					return true;
				}
			}
		}
		return false;
	}
	findUpstreamForSignal(id) {
		if (typeof id !== "string") return void 0;
		for (const upstream of this.upstreams.values()) if (isUpstreamId(upstream.prefix, id)) return upstream;
	}
	findUpstreamForInstance(wireId) {
		for (const upstream of this.upstreams.values()) if (isUpstreamId(upstream.prefix, wireId)) return upstream;
	}
	async handleMessage(clientId, id, method, params) {
		if (method === WATCH_SIGNALS_METHOD) {
			for (const signalId of params) this.reflection.watch(clientId, signalId);
			return;
		}
		if (method === UNWATCH_SIGNALS_METHOD) {
			for (const signalId of params) this.reflection.unwatch(clientId, signalId);
			return;
		}
		try {
			const result = await this.callMethod(method, params);
			const serialized = this.reflection.serialize(result, clientId);
			if (id !== void 0) this.sendResult(clientId, id, serialized);
		} catch (error) {
			if (id !== void 0) this.sendError(clientId, id, {
				code: -1,
				message: error.message
			});
		}
	}
	async callMethod(method, params) {
		const args = params || [];
		let instance = this.root;
		const hashIdx = method.indexOf("#");
		if (hashIdx !== -1) {
			const id = method.slice(0, hashIdx);
			method = method.slice(hashIdx + 1);
			instance = this.instances.get(id);
			if (!instance) throw new Error(`Instance not found: ${id}`);
		}
		const segments = method.split(".");
		const methodName = segments.pop();
		const receiver = segments.length > 0 ? dlv(instance, segments.join(".")) : instance;
		const target = receiver?.[methodName];
		if (typeof target !== "function") throw new Error(`Method not found: ${method}`);
		return target.apply(receiver, args);
	}
	notify(method, params, clientId) {
		const message = formatNotificationMessage(method, params);
		if (clientId) this.clients.get(clientId)?.send(message);
		else for (const transport of this.clients.values()) transport.send(message);
	}
	/** @internal */
	send(clientId, message) {
		const transport = this.clients.get(clientId);
		if (!transport) return;
		transport.send(message);
	}
	sendResult(clientId, id, result) {
		this.send(clientId, formatResultMessage(id, result));
	}
	sendError(clientId, id, error) {
		this.send(clientId, formatErrorMessage(id, error));
	}
};

//#endregion
export { RPC, createMemoryTransportPair, createModel };