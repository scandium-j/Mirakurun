const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { randomUUID } = require("node:crypto");
const { setImmediate: turn } = require("node:timers/promises");
const { RemoteManager } = require("../lib/Mirakurun/remote/RemoteManager");
const { Recovery } = require("../lib/Mirakurun/remote/Recovery");
const { resolveRemoteTuner, validateRemoteConfig, publicServerConfig } = require("../lib/Mirakurun/remote/config");
const shared = require("../lib/Mirakurun/_").default;

function config(role = "child") {
    return { remoteManagementEnabled: true, remoteAutoRestart: true,
        remoteMirakuruns: [{ id: "child", name: "子機", role, host: "127.0.0.1", port: 40772 }] };
}

function environment(t) {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "mirakurun-remote-test-"));
    const original = process.env.SERVER_CONFIG_PATH;
    process.env.SERVER_CONFIG_PATH = path.join(dir, "config", "server.yml");
    t.after(() => {
        if (original === undefined) delete process.env.SERVER_CONFIG_PATH;
        else process.env.SERVER_CONFIG_PATH = original;
        fs.rmSync(dir, { recursive: true, force: true });
    });
    return dir;
}

describe("Remote configuration", () => {
    it("resolves machine IDs with monitoring off and preserves direct configurations", () => {
        const server = config();
        server.remoteManagementEnabled = false;
        assert.equal(resolveRemoteTuner({ remoteMirakurunId: "child" }, server).remoteMirakurunHost, "127.0.0.1");
        const direct = { remoteMirakurunHost: "host.example" };
        assert.equal(resolveRemoteTuner(direct, server), direct);
        assert.throws(() => resolveRemoteTuner({ remoteMirakurunId: "missing" }, server));
        assert.throws(() => resolveRemoteTuner({ remoteMirakurunId: "child", remoteMirakurunHost: "other" }, server));
        assert.throws(() => resolveRemoteTuner({ remoteMirakurunId: "child" }, config("parent")));
    });
    it("rejects ambiguous registrations and unsafe hosts and masks the webhook", () => {
        const server = config();
        validateRemoteConfig(server);
        server.remoteMirakuruns.push({ ...server.remoteMirakuruns[0], id: "other" });
        assert.throws(() => validateRemoteConfig(server));
        server.remoteMirakuruns.pop();
        server.remoteMirakuruns[0].host = "host; shutdown";
        assert.throws(() => validateRemoteConfig(server));
        const hidden = publicServerConfig({ remoteDiscordWebhook: "https://discord.com/api/webhooks/123/test-token" });
        assert.equal(hidden.remoteDiscordWebhook, undefined);
        assert.equal(hidden.remoteDiscordWebhookConfigured, true);
        assert.throws(() => validateRemoteConfig({ remoteDiscordWebhook: "http://localhost/private" }));
    });
});

describe("Remote monitoring", () => {
    it("shares in-flight checks and throttles request bursts, with five-minute healthy polling", async t => {
        environment(t);
        let now = 1000000;
        let calls = 0;
        let release;
        const request = async () => { calls++; return new Promise(resolve => { release = resolve; }); };
        const manager = new RemoteManager(() => config(), () => now, request);
        manager.start();
        t.after(() => manager.stop());
        const checks = Array.from({ length: 100 }, () => manager.check("child"));
        assert.equal(calls, 1);
        release({ protocol: 1, enabled: true, registered: true, role: "parent" });
        await Promise.all(checks);
        now += 29999;
        await Promise.all(Array.from({ length: 100 }, () => manager.check("child")));
        manager.tick();
        assert.equal(calls, 1);
        now = 1300000;
        manager.tick();
        assert.equal(calls, 2);
        release({ protocol: 1, enabled: true, registered: true, role: "parent" });
        await manager.check("child");
        await manager.stop();
        now += 300000;
        manager.tick();
        assert.equal(calls, 2);
    });
    it("does no polling with management disabled", async t => {
        environment(t);
        let calls = 0;
        const manager = new RemoteManager(() => ({ ...config(), remoteManagementEnabled: false }), Date.now, async () => { calls++; });
        manager.start();
        await manager.check("child");
        assert.equal(calls, 0);
        await manager.stop();
    });
    it("notifies once per outage and recovery, without turning ping success into a restart", async t => {
        environment(t);
        let now = 1000000;
        let online = false;
        const notices = [];
        let restarts = 0;
        const settings = { ...config(), remoteDiscordWebhook: "https://discord.com/api/webhooks/123/test-token" };
        const manager = new RemoteManager(() => settings, () => now, async (url, method, body) => {
            if (url.startsWith("https://discord.com/")) { notices.push(body); return {}; }
            if (!online) throw new Error("API timeout (5s)");
            return { protocol: 1, enabled: true, registered: true, role: "parent" };
        }, async () => true, () => { restarts++; }, () => true);
        manager.start();
        t.after(() => manager.stop());
        await manager.check("child");
        for (let i = 0; i < 4; i++) { now += 30000; await manager.check("child"); }
        await manager.notificationQueue;
        assert.equal(notices.length, 1);
        assert.equal(manager.status().peers[0].ping, "ok");
        assert.equal(restarts, 0);
        online = true;
        now += 30000;
        await manager.check("child");
        await manager.notificationQueue;
        assert.equal(notices.length, 2);
        assert.deepEqual(notices[0].allowed_mentions, { parse: [] });
        assert.match(notices[1].content, /最終正常応答: 1970\/01\/01 09:19:10 JST/);
        assert.doesNotMatch(notices[1].content, /T00:19:10.*Z/);
    });
    it("counts at most one stream failure per peer per 30 seconds", async t => {
        environment(t);
        let now = 1000000;
        const manager = new RemoteManager(() => config(), () => now, async () => ({ protocol: 1, enabled: true, registered: true, role: "parent" }));
        manager.start();
        t.after(() => manager.stop());
        await manager.check("child");
        for (let i = 0; i < 100; i++) {
            manager.streamEvent("127.0.0.1", 40772, String(i), { state: "failed", id: randomUUID(), retryable: true });
        }
        assert.equal(manager.peers.get("child").attempts.length, 1);
        now += 30000;
        manager.streamEvent("127.0.0.1", 40772, "0", { state: "failed", id: randomUUID(), retryable: true });
        assert.equal(manager.peers.get("child").attempts.length, 2);
        await turn();
    });
});

describe("Child recovery", () => {
    it("persists incident and hourly limits across restarts", t => {
        const dir = environment(t);
        const file = path.join(dir, "recovery.json");
        let now = 1000000;
        let recovery = new Recovery(file, () => now);
        assert.throws(() => recovery.reserve("test"));
        now += 120000;
        recovery.reserve("test", ["parent/GR/11"]);
        recovery = new Recovery(file, () => now);
        assert.equal(recovery.status(true, true).incidentAttempts, 1);
        recovery.recovered("parent/GR/12");
        assert.equal(recovery.status(true, true).incidentAttempts, 1, "unrelated streams do not reset the incident");
        now += 600000;
        recovery.reserve("test");
        now += 600000;
        assert.throws(() => recovery.reserve("test"));
        recovery.recovered();
        assert.ok(recovery.status(true, true).nextAllowedAt > now, "hourly cooldown is visible after a manual reset");
        assert.throws(() => recovery.reserve("test"), "hourly limit survives incident reset");
        now += 3600000;
        recovery.reserve("new incident");
    });
    it("fails closed on corrupt recovery history", t => {
        const dir = environment(t);
        const file = path.join(dir, "recovery.json");
        fs.writeFileSync(file, "not json");
        const recovery = new Recovery(file);
        assert.equal(recovery.status(true, true).blocked, true);
        assert.throws(() => recovery.reserve("test"));
    });
    it("requires matching child-side failed requests and an online registered parent", async t => {
        environment(t);
        let now = 1000000;
        let restarted = false;
        const manager = new RemoteManager(() => config("parent"), () => now,
            async () => ({ protocol: 1, enabled: true, registered: true, role: "child" }), async () => true,
            () => { restarted = true; }, () => true);
        manager.start();
        t.after(() => manager.stop());
        const originalTuner = shared.tuner;
        shared.tuner = { devices: [] };
        t.after(() => { shared.tuner = originalTuner; });
        await manager.check("child");
        now += 120000;
        const ids = Array.from({ length: 3 }, () => randomUUID());
        for (const id of ids) {
            manager.beginIncoming(id, "127.0.0.1", () => {});
            manager.incomingStage(id, "closed");
        }
        await assert.rejects(manager.recover("127.0.0.1", ids), /失敗連打/);
        for (const id of ids) {
            now += 30000;
            manager.incomingStage(id, "closed");
        }
        await assert.rejects(manager.recover("127.0.0.2", ids));
        await assert.rejects(manager.recover("127.0.0.1", [randomUUID(), randomUUID(), randomUUID()]));
        shared.tuner = { devices: [{ users: [{ priority: 0, streamInfo: { 0: { packet: 1 } } }] }] };
        await assert.rejects(manager.recover("127.0.0.1", ids), /録画・視聴/);
        shared.tuner = { devices: [] };
        assert.match(await manager.recover("127.0.0.1", ids), /受け付け/);
        assert.equal(manager.status().recovery.incidentAttempts, 0, "a cancelled reservation does not consume a restart");
        await manager.stop();
        assert.equal(restarted, false, "shutdown cancels pending restart");
        await assert.rejects(manager.recover("127.0.0.1", ids));
    });
    it("rechecks active recordings immediately before restarting and saves the limit first", async t => {
        const dir = environment(t);
        t.mock.timers.enable({ apis: ["setTimeout"] });
        let now = 1000000;
        let restarted = 0;
        const manager = new RemoteManager(() => config("parent"), () => now,
            async () => ({ protocol: 1, enabled: true, registered: true, role: "child" }), async () => true,
            () => {
                const saved = JSON.parse(fs.readFileSync(path.join(dir, "remote", "recovery.json"), "utf8"));
                assert.equal(saved.incidentAttempts, 1);
                restarted++;
            }, () => true);
        const original = shared.tuner;
        shared.tuner = { devices: [] };
        t.after(async () => { await manager.stop(); shared.tuner = original; });
        manager.start();
        await manager.check("child");
        now += 120000;
        const ids = Array.from({ length: 3 }, () => randomUUID());
        for (const id of ids) {
            manager.beginIncoming(id, "127.0.0.1", () => {});
            manager.incomingStage(id, "closed");
            now += 30000;
        }
        await manager.recover("127.0.0.1", ids);
        shared.tuner = { devices: [{ users: [{ priority: 0, streamInfo: { 0: { packet: 1 } } }] }] };
        t.mock.timers.tick(1000);
        assert.equal(restarted, 0);
        assert.equal(manager.status().recovery.incidentAttempts, 0);
        shared.tuner = { devices: [] };
        await manager.recover("127.0.0.1", ids);
        t.mock.timers.tick(1000);
        assert.equal(restarted, 1);
    });
    it("does not turn a rejected tuner request into a recoverable failure when it closes", async t => {
        environment(t);
        const manager = new RemoteManager(() => config("parent"), Date.now, async () => ({ protocol: 1 }));
        manager.start();
        t.after(() => manager.stop());
        const id = randomUUID();
        let priority = 0;
        manager.beginIncoming(id, "127.0.0.1", value => { priority = value; });
        assert.equal(manager.updatePriority(id, "127.0.0.2", 2), false);
        assert.equal(manager.updatePriority(id, "127.0.0.1", 2), true);
        assert.equal(priority, 2);
        manager.incomingStage(id, "failed", false);
        manager.incomingStage(id, "closed");
        assert.equal(manager.incoming.get(id).retryable, false);
        assert.equal(manager.updatePriority(id, "127.0.0.1", 3), false);
    });
});
