/*
 * Copyright 2026 nyanz00
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
 * Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */
import * as React from "react";
import { useEffect, useRef, useState } from "react";
import { DateTime } from "luxon";
import { Button, Callout, Dialog, DialogBody, DialogFooter, FormGroup, HTMLSelect, InputGroup, Spinner, Switch, Tag } from "@blueprintjs/core";
import { ConfigServer, RemoteMirakurun, RemoteStatus } from "../../../api";
import { state } from "../modules/state";
import "./RemoteSection.sass";

const labels = { unknown: "未確認", online: "正常", suspect: "再確認中", offline: "応答なし", error: "設定エラー",
    idle: "未使用", starting: "開始待ち", receiving: "受信中", failed: "失敗" };
const connectionLabels = {
    online: "双方向: 正常",
    "outgoing-error": "接続: 自分→相手に異常",
    "incoming-error": "接続: 相手→自分に異常",
    "both-error": "接続: 双方向に異常",
    unknown: "接続: 未確認",
    error: "接続: 設定エラー"
};
const time = (value?: number) => value ? DateTime.fromMillis(value).setZone("Asia/Tokyo").setLocale("ja").toFormat("MM/dd HH:mm:ss") : "未確認";

async function json(url: string, method = "GET", body?: unknown, signal?: AbortSignal) {
    const response = await fetch(url, { method, signal, headers: { "Content-Type": "application/json" }, body: body === undefined ? undefined : JSON.stringify(body) });
    const result = await response.json();
    if (!response.ok) throw new Error(result.reason || `HTTP ${response.status}`);
    return result;
}

export const RemoteSection: React.FC = () => {
    const [status, setStatus] = useState<RemoteStatus>(null);
    const [error, setError] = useState("");
    const [editing, setEditing] = useState<ConfigServer>(null);
    const [webhook, setWebhook] = useState("");
    const [clearWebhook, setClearWebhook] = useState(false);
    const [saving, setSaving] = useState(false);
    const [message, setMessage] = useState("");
    const [checking, setChecking] = useState("");
    const [resetOpen, setResetOpen] = useState(false);
    const lifetime = useRef<AbortController>();
    const active = () => lifetime.current && !lifetime.current.signal.aborted;
    const request = (url: string, method = "GET", body?: unknown) => json(url, method, body, lifetime.current?.signal);

    useEffect(() => {
        let disposed = false;
        let busy = false;
        const controller = new AbortController();
        lifetime.current = controller;
        const refresh = async () => {
            if (busy || document.hidden) return;
            busy = true;
            try {
                const response = await fetch("/api/remote", { signal: controller.signal });
                if (!response.ok) throw new Error(`HTTP ${response.status}`);
                const value = await response.json();
                if (!disposed) { setStatus(value); setError(""); }
            } catch (e) {
                if (!disposed) setError("Remote状態を取得できませんでした。次の更新で再試行します。");
            } finally { busy = false; }
        };
        void refresh();
        const timer = setInterval(refresh, 5000);
        return () => { disposed = true; clearInterval(timer); controller.abort(); };
    }, []);

    const openConfig = async () => {
        try {
            const config = await request("/api/config/server");
            if (!active()) return;
            setEditing(config);
            setWebhook(""); setClearWebhook(false); setMessage("");
        } catch (e) { if (active()) setError(e.message); }
    };
    const updatePeer = (index: number, patch: Partial<RemoteMirakurun>) => {
        setEditing({ ...editing, remoteMirakuruns: editing.remoteMirakuruns.map((peer, i) => i === index ? { ...peer, ...patch } : peer) });
    };
    const save = async () => {
        setSaving(true);
        try {
            const config = { ...editing };
            delete config.remoteDiscordWebhookConfigured;
            if (clearWebhook) config.remoteDiscordWebhook = "";
            else if (webhook.trim()) config.remoteDiscordWebhook = webhook.trim();
            const result = await request("/api/config/server", "PUT", config);
            if (!active()) return;
            state.serverConfig = result;
            setEditing(null); setWebhook("");
            setMessage("保存しました。設定の反映には、このMirakurunの再起動が必要です。");
        } catch (e) { if (active()) setMessage(e.message); }
        finally { if (active()) setSaving(false); }
    };
    const check = async (id: string) => {
        setChecking(id);
        try {
            await request("/api/remote/check", "POST", { id });
            const value = await request("/api/remote");
            if (active()) setStatus(value);
        } catch (e) { if (active()) setError(e.message); }
        finally { if (active()) setChecking(""); }
    };

    return <div className="remote-section">
        <div className="remote-toolbar">
            <Tag intent={status?.enabled ? "success" : "none"}>{!status ? "状態未取得" : status.enabled ? "接続管理 ON" : "接続管理 OFF"}</Tag>
            <Button icon="cog" onClick={openConfig} disabled={!status?.canManage}>接続先・通知設定</Button>
        </div>
        {error && <Callout intent="danger">{error}</Callout>}
        {message && !editing && <Callout>{message}</Callout>}
        {!status && !error && <Spinner size={20} />}
        {status?.notificationError && <Callout intent="warning">{status.notificationError}</Callout>}
        {status && status.peers.length === 0 && <p className="bp5-text-muted">接続先マシンは未登録です。</p>}
        {status?.peers.map(peer => <div className="remote-machine" key={peer.id}>
            <div className="remote-toolbar">
                <strong>{peer.name}</strong><Tag>{peer.role === "child" ? "子機" : "親機"}</Tag>
                <Tag intent={peer.connectionState === "online" ? "success" : peer.connectionState === "error" || peer.connectionState === "outgoing-error" || peer.connectionState === "incoming-error" || peer.connectionState === "both-error" ? "danger" : "none"}>
                    {connectionLabels[peer.connectionState || "unknown"]}
                </Tag>
                <Tag intent={peer.state === "online" ? "success" : peer.state === "offline" || peer.state === "error" ? "danger" : "none"}>
                    自分→相手: {labels[peer.state]}
                </Tag>
                <Tag intent={peer.reverse?.state === "online" ? "success" : peer.reverse?.state === "offline" || peer.reverse?.state === "error" ? "danger" : "none"}>
                    相手→自分: {labels[peer.reverse?.state || "unknown"]}
                </Tag>
                {peer.role === "child" && <Tag>配信: {labels[peer.streamState]}</Tag>}
                <Button small icon="refresh" loading={checking === peer.id} disabled={!status.enabled || !status.canManage || !!checking}
                    onClick={() => check(peer.id)}>今すぐ確認</Button>
            </div>
            <div>{peer.host}:{peer.port || 40772}</div>
            <div className="remote-times">自分→相手 最終成功: {time(peer.lastSuccess)} ／ 相手→自分 最終受信: {time(peer.inboundAt)}</div>
            {(peer.error || peer.streamError) && <Callout intent="warning">{peer.error}{peer.error && peer.streamError && <br />}{peer.streamError}</Callout>}
            <details>
                <summary>詳細・再起動履歴</summary>
                <p>自分→相手 API: {labels[peer.state]} ／ 最終確認: {time(peer.checkedAt)} ／ 連続失敗: {peer.failures}回 ／ ping: {peer.ping}</p>
                <p>相手→自分: {labels[peer.reverse?.state || "unknown"]} ／ 最終確認: {time(peer.reverse?.checkedAt)} ／ 最終成功: {time(peer.reverse?.lastSuccess)} ／ 連続失敗: {peer.reverse ? `${peer.reverse.failures}回` : "未確認"}</p>
                {peer.reverse?.error && <Callout intent="warning">相手→自分: {peer.reverse.error}</Callout>}
                <p>復旧処理: {peer.recovery || "待機中"}</p>
                {peer.lastRequest && <p>子機の要求記録: {peer.lastRequest.stage} / {time(peer.lastRequest.at)} / {peer.lastRequest.id}</p>}
                {peer.remoteRecovery && <>
                    <p>相手の自動再起動: {peer.remoteRecovery.enabled ? "ON" : "OFF"}
                        {!peer.remoteRecovery.supported && "（起動方式が未対応）"}
                        {peer.remoteRecovery.blocked && "（停止中・上限到達）"} ／ 同一障害: {peer.remoteRecovery.incidentAttempts}/2回</p>
                    {peer.remoteRecovery.error && <p>{peer.remoteRecovery.error}</p>}
                    <ul>{peer.remoteRecovery.history.map((entry, i) => <li key={i}>{time(entry.at)} {entry.reason}</li>)}</ul>
                </>}
            </details>
        </div>)}
        {status && <details>
            <summary>このマシンの自動再起動</summary>
            <p>{status.recovery.enabled ? "ON" : "OFF"} ／ 同一障害: {status.recovery.incidentAttempts}/2回
                {status.recovery.blocked && " ／ 上限到達・停止中"}
                {!status.recovery.supported && " ／ Windowsサービス／systemd起動時のみ対応"}</p>
            {status.recovery.error && <Callout intent="danger">{status.recovery.error}</Callout>}
            <ul>{status.recovery.history.map((entry, i) => <li key={i}>{time(entry.at)} {entry.reason}</li>)}</ul>
            <Button disabled={!status.canManage || status.recovery.incidentAttempts === 0} onClick={() => setResetOpen(true)}>障害ごとの回数制限を解除</Button>
        </details>}
        <Dialog isOpen={resetOpen} title="再起動の回数制限を解除" onClose={() => setResetOpen(false)}>
            <DialogBody>同じ障害について自動再起動を再び許可します。直近1時間の上限と再起動間隔は維持されます。</DialogBody>
            <DialogFooter actions={<><Button onClick={() => setResetOpen(false)}>キャンセル</Button><Button intent="warning" onClick={async () => {
                try { await request("/api/remote/reset", "POST", {}); if (active()) setResetOpen(false); }
                catch (e) { if (active()) setError(e.message); }
            }}>解除</Button></>} />
        </Dialog>
        <Dialog isOpen={!!editing} title="Remote設定" className="remote-config-dialog" onClose={() => !saving && setEditing(null)}>
            <DialogBody>
                {editing && <>
                    <Switch checked={editing.remoteManagementEnabled ?? false} label="接続管理を有効にする"
                        onChange={e => setEditing({ ...editing, remoteManagementEnabled: e.currentTarget.checked })} />
                    <p>接続確認は起動時・異常時30秒、正常時5分間隔です。接続先にも自機を登録し、通常のAPI接続許可を設定してください。</p>
                    <p>「今すぐ確認」や配信失敗時の確認も、同じマシンには最短30秒間隔で行います。</p>
                    {(editing.remoteMirakuruns || []).map((peer, i) => <div className="remote-machine remote-config-grid" key={i}>
                        <FormGroup label="識別ID" helperText="チューナーから参照する名前（英数字・_・-）">
                            <InputGroup value={peer.id} onChange={e => updatePeer(i, { id: e.target.value })} />
                        </FormGroup>
                        <FormGroup label="表示名"><InputGroup value={peer.name} onChange={e => updatePeer(i, { name: e.target.value })} /></FormGroup>
                        <FormGroup label="相手の役割">
                            <HTMLSelect value={peer.role} onChange={e => updatePeer(i, { role: e.target.value as "parent" | "child" })}>
                                <option value="child">子機：チューナーを利用する相手</option>
                                <option value="parent">親機：自機のチューナーを利用する相手</option>
                            </HTMLSelect>
                        </FormGroup>
                        <FormGroup label="IPアドレス・ホスト名"><InputGroup value={peer.host} onChange={e => updatePeer(i, { host: e.target.value })} /></FormGroup>
                        <FormGroup label="ポート"><InputGroup type="number" value={String(peer.port ?? 40772)}
                            onChange={e => updatePeer(i, { port: Number(e.target.value) })} /></FormGroup>
                        <Button icon="trash" onClick={() => setEditing({ ...editing, remoteMirakuruns: editing.remoteMirakuruns.filter((_, index) => index !== i) })}>削除</Button>
                    </div>)}
                    <Button icon="add" onClick={() => setEditing({ ...editing, remoteMirakuruns: [...(editing.remoteMirakuruns || []), {
                        id: "", name: "", role: "child", host: "", port: 40772
                    }] })}>マシンを追加</Button>
                    <hr />
                    <Switch checked={editing.remoteAutoRestart ?? false} label="子機として配信開始に失敗したときや、親機からのAPI通信異常が続くとき、このMirakurunの自動再起動を許可する"
                        onChange={e => setEditing({ ...editing, remoteAutoRestart: e.currentTarget.checked })} />
                    <p>Windowsサービス／systemdから起動している場合に対応。起動後2分待機、再起動間隔10分、同一障害2回・1時間2回まで。ほかの録画・視聴中は保留します。</p>
                    <FormGroup label="Discord Webhook（親機側の通知）" helperText={editing.remoteDiscordWebhookConfigured ? "登録済み。空欄のままなら現在のURLを維持します。" : "空欄なら通知しません。"}>
                        <InputGroup type="password" autoComplete="off" value={webhook} onChange={e => setWebhook(e.target.value)} disabled={clearWebhook} />
                    </FormGroup>
                    {editing.remoteDiscordWebhookConfigured && <Switch checked={clearWebhook} label="登録済みWebhookを削除する" onChange={e => setClearWebhook(e.currentTarget.checked)} />}
                    {message && <Callout intent="warning">{message}</Callout>}
                </>}
            </DialogBody>
            <DialogFooter actions={<><Button disabled={saving} onClick={() => setEditing(null)}>キャンセル</Button><Button intent="primary" loading={saving} onClick={save}>保存</Button></>} />
        </Dialog>
    </div>;
};
