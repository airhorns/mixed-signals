//#region shared/protocol.ts
const ROOT_NOTIFICATION_METHOD = "@R";
const SIGNAL_UPDATE_METHOD = "@S";
const WATCH_SIGNALS_METHOD = "@W";
const UNWATCH_SIGNALS_METHOD = "@U";
function parseMessageId(value) {
	if (value === "") return void 0;
	const parsed = Number(value);
	return Number.isInteger(parsed) ? parsed : void 0;
}
function parseWireMessage(message) {
	if (message.length < 2) return null;
	const type = message[0];
	if (type === "R" || type === "E") {
		const separatorIndex = message.indexOf(":");
		if (separatorIndex === -1) return null;
		const id = parseMessageId(message.slice(1, separatorIndex));
		if (id === void 0) return null;
		return {
			type: type === "R" ? "result" : "error",
			id,
			payload: message.slice(separatorIndex + 1)
		};
	}
	if (type !== "M" && type !== "N") return null;
	const methodSeparatorIndex = message.indexOf(":", 1);
	if (methodSeparatorIndex === -1) return null;
	const payloadSeparatorIndex = message.indexOf(":", methodSeparatorIndex + 1);
	if (payloadSeparatorIndex === -1) return null;
	const id = message.slice(1, methodSeparatorIndex);
	const method = message.slice(methodSeparatorIndex + 1, payloadSeparatorIndex);
	const payload = message.slice(payloadSeparatorIndex + 1);
	if (!method) return null;
	if (type === "M") {
		const parsedId = parseMessageId(id);
		if (parsedId === void 0) return null;
		return {
			type: "call",
			id: parsedId,
			method,
			payload
		};
	}
	if (id !== "") return null;
	return {
		type: "notification",
		method,
		payload
	};
}
function parseWireParams(payload, reviver) {
	return JSON.parse(payload ? `[${payload}]` : "[]", reviver);
}
function parseWireValue(payload, reviver) {
	return JSON.parse(payload, reviver);
}
function stringifyWireParams(params = []) {
	return params.map((param) => JSON.stringify(param)).join(",");
}
function formatCallMessage(id, method, params = []) {
	return `M${id}:${method}:${stringifyWireParams(params)}`;
}
function formatNotificationMessage(method, params = []) {
	return `N:${method}:${stringifyWireParams(params)}`;
}
function formatResultMessage(id, result) {
	return `R${id}:${JSON.stringify(result)}`;
}
function formatErrorMessage(id, error) {
	return `E${id}:${JSON.stringify(error)}`;
}

//#endregion
export { formatCallMessage as a, formatResultMessage as c, parseWireValue as d, WATCH_SIGNALS_METHOD as i, parseWireMessage as l, SIGNAL_UPDATE_METHOD as n, formatErrorMessage as o, UNWATCH_SIGNALS_METHOD as r, formatNotificationMessage as s, ROOT_NOTIFICATION_METHOD as t, parseWireParams as u };