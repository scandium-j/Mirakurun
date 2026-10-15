const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const http = require("node:http");
const { once } = require("node:events");
const { randomUUID } = require("node:crypto");
const { RemoteManager } = require("../lib/Mirakurun/remote/RemoteManager");
const shared = require("../lib/Mirakurun/_").default;

function fixture(t, options = {}) {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "mirakurun-direction-"));
    const previousPath = process.env.SERVER_CONFIG_PATH;
    const previousTuner = shared.tuner;
    process.env.SERVER_CONFIG_PATH = path.join(directory, "config", "server.yml");
    shared.tuner = { devices: [] };
    let now = 1000000;
    let sequence = 0;
    let restarts = 0;
    const instanceId = randomUUID();
    const settings = { remoteManagementEnabled: true, remoteAutoRestart: true,
        remoteMirakuruns: [{ id: "peer", name: "Parent", role: "parent", host: "127.0.0.1", port: 40772 }],
        ...options.settings };
    let observation = { state: "offline", failures: 3, error: "API timeout (5s)", ...options.observation };
    let health = { protocol: 1, enabled: true, registered: true, role: "child", ...options.health };
    let failOutgoing = false;
    const manager = new RemoteManager(() => settings, () => now, async () => {
        if (failOutgoing) throw new Error("API timeout (5s)");
        return { ...health, checkedAt: 9000000000000, observation: {
            instanceId, sequence: ++sequence, ageMs: 0, successAgeMs: null, ...observation
        } };
    }, async () => true, () => {
        const saved = JSON.parse(fs.readFileSync(path.join(directory, "remote", "recovery.json"), "utf8"));
        assert.equal(saved.incidentAttempts, restarts + 1, "restart allowance is saved before exit");
        restarts++;
    }, () => options.supported !== false);
    manager.start();
    t.after(async () => {
        await manager.stop();
        shared.tuner = previousTuner;
        if (previousPath === undefined) delete process.env.SERVER_CONFIG_PATH;
        else process.env.SERVER_CONFIG_PATH = previousPath;
        fs.rmSync(directory, { recursive: true, force: true });
    });
    return { manager, settings, directory, get now() { return now; }, get restarts() { return restarts; },
        observation(value) { observation = { ...observation, ...value }; },
        health(value) { health = { ...health, ...value }; },
        fail(value) { failOutgoing = value; },
        async check(delta = 30000) { now += delta; await manager.check("peer"); } };
}

describe("Remote direction monitoring", () => {
    it("distinguishes healthy, one-way, both-way, configuration, unknown and stale results without feedback loops", async t => {
        const f = fixture(t, { settings: { remoteAutoRestart: false },
            observation: { state: "online", failures: 0, error: null, successAgeMs: 0 } });
        await f.manager.check("peer");
        assert.equal(f.manager.status().peers[0].connectionState, "online");
        assert.equal(f.manager.status().peers[0].reverse.checkedAt, f.now, "remote clock offsets do not change result freshness");
        assert.equal((await f.manager.health("127.0.0.1")).observation.state, "online");
        f.observation({ state: "offline", failures: 3, error: "API timeout (5s)" });
        await f.check();
        assert.equal(f.manager.status().peers[0].state, "online", "raw outgoing health stays independent");
        assert.equal(f.manager.status().peers[0].connectionState, "incoming-error");
        assert.equal((await f.manager.health("127.0.0.1")).observation.state, "online", "combined errors are never echoed as outgoing failures");
        f.fail(true);
        await f.check();
        assert.equal(f.manager.status().peers[0].connectionState, "both-error");
        f.fail(false);
        f.observation({ state: "error", error: "HTTP 403" });
        await f.check();
        assert.equal(f.manager.status().peers[0].connectionState, "error");
        f.observation({ state: "unknown", failures: 0, error: null });
        await f.check();
        assert.equal(f.manager.status().peers[0].connectionState, "unknown");
        f.observation({ state: "online", failures: 0, error: null, ageMs: 360001, successAgeMs: 360001 });
        await f.check();
        assert.equal(f.manager.status().peers[0].connectionState, "stale");
        f.fail(true);
        await f.check();
        assert.equal(f.manager.status().peers[0].connectionState, "outgoing-error");
        assert.equal("observation" in f.manager.status().peers[0], false, "internal evidence is not exposed in status");
    });

    it("does not refresh repeated observations or mistake old peers for bidirectionally healthy ones", async t => {
        const f = fixture(t, { observation: { sequence: 1, state: "online", failures: 0, error: null } });
        await f.manager.check("peer");
        await f.check(360001);
        assert.equal(f.manager.status().peers[0].reverse.state, "stale");
        f.observation({ sequence: 2, successAgeMs: undefined });
        await f.check();
        assert.equal(f.manager.status().peers[0].reverse.state, "unknown", "missing observation fields are not assumed healthy");
        assert.equal(f.restarts, 0);
    });

    it("uses 30-second retries for reverse failures and five-minute polling after both directions recover", async t => {
        const f = fixture(t, { settings: { remoteAutoRestart: false } });
        await f.manager.check("peer");
        assert.equal(f.manager.peers.get("peer").nextCheck - f.now, 30000);
        f.observation({ state: "online", failures: 0, error: null, successAgeMs: 0 });
        await f.check();
        assert.equal(f.manager.peers.get("peer").nextCheck - f.now, 300000);
    });
});

describe("Child self recovery", () => {
    it("detects a real one-way HTTP failure through the parent health response and schedules child recovery", async t => {
        t.mock.timers.enable({ apis: ["setTimeout"] });
        const directory = fs.mkdtempSync(path.join(os.tmpdir(), "mirakurun-one-way-"));
        const previousPath = process.env.SERVER_CONFIG_PATH;
        const previousTuner = shared.tuner;
        process.env.SERVER_CONFIG_PATH = path.join(directory, "config", "server.yml");
        shared.tuner = { devices: [] };
        let parent;
        let child;
        let now = 1000000;
        let blocked = true;
        let restarts = 0;
        const parentServer = http.createServer(async (req, res) => {
            res.setHeader("Content-Type", "application/json");
            res.end(JSON.stringify(await parent.health(req.socket.remoteAddress)));
        });
        const childServer = http.createServer(async (req, res) => {
            if (blocked) {
                req.socket.destroy();
                return;
            }
            res.setHeader("Content-Type", "application/json");
            res.end(JSON.stringify(await child.health(req.socket.remoteAddress)));
        });
        parentServer.listen(0, "127.0.0.1");
        childServer.listen(0, "127.0.0.1");
        await Promise.all([once(parentServer, "listening"), once(childServer, "listening")]);
        const settings = (role, port) => ({ remoteManagementEnabled: true, remoteAutoRestart: true,
            remoteMirakuruns: [{ id: "peer", name: role, role, host: "127.0.0.1", port }] });
        parent = new RemoteManager(() => settings("child", childServer.address().port), () => now,
            undefined, async () => true, () => assert.fail("The parent must not restart"), () => true);
        child = new RemoteManager(() => settings("parent", parentServer.address().port), () => now,
            undefined, async () => true, () => {
                assert.equal(JSON.parse(fs.readFileSync(path.join(directory, "remote", "recovery.json"), "utf8")).incidentAttempts, 1);
                restarts++;
            }, () => true);
        t.after(async () => {
            await Promise.all([parent.stop(), child.stop()]);
            for (const server of [parentServer, childServer]) {
                server.closeAllConnections();
                await new Promise(resolve => server.close(resolve));
            }
            shared.tuner = previousTuner;
            if (previousPath === undefined) delete process.env.SERVER_CONFIG_PATH;
            else process.env.SERVER_CONFIG_PATH = previousPath;
            fs.rmSync(directory, { recursive: true, force: true });
        });
        parent.start();
        await parent.check("peer");
        child.start();
        await child.check("peer");
        for (const delta of [120000, 30000, 30000, 30000]) {
            now += delta;
            await parent.check("peer");
            await child.check("peer");
        }
        assert.match(parent.status().peers[0].error, /ECONNRESET/);
        assert.equal(child.status().peers[0].state, "online");
        assert.equal(child.status().peers[0].connectionState, "incoming-error");
        assert.ok(child.restartTimer);
        t.mock.timers.tick(1000);
        assert.equal(restarts, 1);
        blocked = false;
        now += 30000;
        await parent.check("peer");
        await child.check("peer");
        assert.equal(child.status().peers[0].connectionState, "online");
        assert.equal(parent.status().peers[0].connectionState, "online");
    });

    it("requires new, sustained reverse failures, the startup grace, and persists the ten-minute and incident limits", async t => {
        t.mock.timers.enable({ apis: ["setTimeout"] });
        const f = fixture(t);
        await f.manager.check("peer");
        for (let i = 0; i < 3; i++) {
            await f.check();
            t.mock.timers.tick(1000);
            assert.equal(f.restarts, 0, "the first two minutes never trigger a restart");
        }
        await f.check();
        assert.ok(f.manager.restartTimer);
        t.mock.timers.tick(1000);
        assert.equal(f.restarts, 1);
        await f.check();
        t.mock.timers.tick(1000);
        assert.equal(f.restarts, 1, "reverse failure does not bypass the ten-minute cooldown");
        assert.match(f.manager.status().peers[0].recovery, /制限/);
        await f.check(600000);
        await f.check();
        await f.check();
        t.mock.timers.tick(1000);
        assert.equal(f.restarts, 2);
        await f.check(600000);
        await f.check();
        await f.check();
        t.mock.timers.tick(1000);
        assert.equal(f.restarts, 2, "the incident limit prevents a third restart");
        f.manager.resetRecovery();
        await f.check();
        t.mock.timers.tick(1000);
        assert.equal(f.restarts, 2, "resetting the incident does not erase the hourly limit");
    });

    it("does not self-restart for disabled, unsupported, stale, replayed, unregistered or non-network failures", async t => {
        for (const [name, options] of [
            ["disabled", { settings: { remoteAutoRestart: false } }],
            ["management off", { settings: { remoteManagementEnabled: false } }],
            ["unsupported service", { supported: false }],
            ["stale report", { observation: { ageMs: 60001 } }],
            ["same report replayed", { observation: { sequence: 1 } }],
            ["unregistered", { health: { registered: false } }],
            ["wrong role", { health: { role: "parent" } }],
            ["management off at parent", { health: { enabled: false } }],
            ["authorization failure", { observation: { error: "HTTP 403" } }],
            ["incoming IP denied", { settings: { allowIPv4CidrRanges: ["192.168.0.0/24"], allowIPv6CidrRanges: [] } }],
            ["invalid response", { observation: { error: "Invalid JSON response" } }],
            ["no completed checks", { observation: { sequence: 0 } }],
            ["insufficient failures", { observation: { failures: 2 } }]
        ]) {
            await t.test(name, async sub => {
                sub.mock.timers.enable({ apis: ["setTimeout"] });
                const f = fixture(sub, options);
                await f.manager.check("peer");
                await f.check(120000);
                await f.check();
                await f.check();
                sub.mock.timers.tick(1000);
                assert.equal(f.restarts, 0);
            });
        }
    });

    it("clears consecutive evidence on outgoing loss, and on a peer process restart", async t => {
        t.mock.timers.enable({ apis: ["setTimeout"] });
        const f = fixture(t);
        await f.manager.check("peer");
        await f.check(120000);
        await f.check();
        f.fail(true);
        await f.check();
        assert.equal(f.manager.peers.get("peer").reverseEvidence.length, 0);
        f.fail(false);
        await f.check();
        f.observation({ instanceId: randomUUID() });
        await f.check();
        assert.equal(f.manager.peers.get("peer").reverseEvidence.length, 1);
        t.mock.timers.tick(1000);
        assert.equal(f.restarts, 0);
    });

    it("defers active recordings and cancels a queued restart when a recording starts, incoming health returns, or settings change", async t => {
        t.mock.timers.enable({ apis: ["setTimeout"] });
        const f = fixture(t);
        const active = { devices: [{ users: [{ priority: 0, streamInfo: { 0: { packet: 1 } } }] }] };
        shared.tuner = active;
        await f.manager.check("peer");
        await f.check(120000);
        await f.check();
        await f.check();
        assert.equal(f.manager.restartTimer, undefined);
        shared.tuner = { devices: [] };
        await f.check();
        assert.ok(f.manager.restartTimer);
        shared.tuner = active;
        t.mock.timers.tick(1000);
        assert.equal(f.restarts, 0);
        shared.tuner = { devices: [] };
        await f.check();
        assert.ok(f.manager.restartTimer);
        await f.manager.health("127.0.0.1");
        t.mock.timers.tick(1000);
        assert.equal(f.restarts, 0, "new incoming health cancels a pending self-restart");
        await f.check();
        assert.ok(f.manager.restartTimer);
        f.settings.remoteAutoRestart = false;
        t.mock.timers.tick(1000);
        assert.equal(f.restarts, 0);
        f.settings.remoteAutoRestart = true;
        await f.check();
        assert.ok(f.manager.restartTimer);
        await f.manager.stop();
        t.mock.timers.tick(1000);
        assert.equal(f.restarts, 0);
    });

    it("clears only the API incident after sustained bidirectional recovery", async t => {
        const f = fixture(t);
        await f.manager.check("peer");
        await f.check(120000);
        f.manager.recovery.reserve("API outage", ["api:peer"]);
        f.observation({ state: "online", failures: 0, error: null, ageMs: 300000, successAgeMs: 300000 });
        await f.check();
        assert.equal(f.manager.status().recovery.incidentAttempts, 1);
        await f.check(60000);
        assert.equal(f.manager.status().recovery.incidentAttempts, 0);
        await f.check(600000);
        f.manager.recovery.reserve("stream outage", ["127.0.0.1/GR/11"]);
        await f.check();
        await f.check(60000);
        assert.equal(f.manager.status().recovery.incidentAttempts, 1, "API checks never reset stream incident limits");
    });
});
