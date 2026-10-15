/*
   Copyright 2016 kanreisa

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
import * as os from "os";
import * as https from "https";
import { Validator } from "ip-num/Validator";
import { IPv4, IPv6 } from "ip-num/IPNumber";
import { IPv4Prefix, IPv6Prefix } from "ip-num/Prefix";
import { IPv4CidrRange, IPv6CidrRange } from "ip-num/IPRange";
import _ from "./_";

function isTailscaleInterface(name: string): boolean {
    return /^(tailscale|tailscale0)$/i.test(name);
}

export function getTailscaleIPv4AddressesForListen(): string[] {
    const interfaces = os.networkInterfaces();
    return [...new Set(Object.keys(interfaces)
        .filter(isTailscaleInterface)
        .flatMap(name => (interfaces[name] || [])
            .filter(address => address.family === "IPv4" && !address.internal && isPermittedIPAddress(address.address))
            .map(address => address.address)))];
}

export function getIPv4AddressesForListen(excludeTailscale = false): string[] {
    const addresses = [];

    const interfaces = os.networkInterfaces();
    Object.keys(interfaces).filter(name => !excludeTailscale || !isTailscaleInterface(name)).forEach(k => {
        interfaces[k]
            .filter(a => {
                return (
                    a.family === "IPv4" &&
                    a.internal === false &&
                    isPermittedIPAddress(a.address) === true
                );
            })
            .forEach(a => addresses.push(a.address));
    });

    return addresses;
}

export function getIPv6AddressesForListen(): string[] {
    const addresses = [];

    const interfaces = os.networkInterfaces();
    Object.keys(interfaces).forEach(k => {
        interfaces[k]
            .filter(a => {
                return (
                    a.family === "IPv6" &&
                    a.internal === false &&
                    isPermittedIPAddress(a.address) === true
                );
            })
            .forEach(a => addresses.push(a.address + "%" + k));
    });

    return addresses;
}

export function isPermittedIPAddress(addr: string, serverConfig = _.config.server): boolean {
    const [isIPv4] = Validator.isValidIPv4String(addr);
    if (isIPv4) {
        const ipv4 = new IPv4CidrRange(new IPv4(addr), new IPv4Prefix(32));
        for (const rangeString of serverConfig.allowIPv4CidrRanges) {
            if (ipv4.inside(IPv4CidrRange.fromCidr(rangeString))) {
                return true;
            }
        }
    }

    const [isIPv6] = Validator.isValidIPv6String(addr);
    if (isIPv6) {
        const ipv6 = new IPv6CidrRange(new IPv6(addr), new IPv6Prefix(128));
        for (const rangeString of serverConfig.allowIPv6CidrRanges) {
            if (ipv6.inside(IPv6CidrRange.fromCidr(rangeString))) {
                return true;
            }
        }
    }

    return false;
}

export function isPermittedHost(url: string, allowedHostname?: string): boolean {
    const u = new URL(url);

    if (u.hostname === "localhost" || u.hostname === allowedHostname || isPermittedIPAddress(u.hostname) === true) {
        return true;
    }

    return false;
}

export async function getLatestVersion(): Promise<string> {
    return new Promise((resolve, reject) => {
        const req = https.request("https://registry.npmjs.org/mirakurun/latest", {
            headers: {
                "Accept": "application/json",
                "User-Agent": "Mirakurun"
            },
            timeout: 10000
        }, (res) => {
            const chunks: Buffer[] = [];

            res.on("data", (chunk) => {
                chunks.push(Buffer.from(chunk));
            });

            res.on("end", () => {
                if (res.statusCode !== 200) {
                    reject(new Error(`npm registry responded with ${res.statusCode}`));
                    return;
                }

                try {
                    const data = JSON.parse(Buffer.concat(chunks).toString("utf8"));
                    if (typeof data.version !== "string") {
                        reject(new Error("npm registry response does not include version"));
                        return;
                    }

                    resolve(data.version);
                } catch (e) {
                    reject(e);
                }
            });
        });

        req.on("timeout", () => {
            req.destroy(new Error("npm registry request timed out"));
        });
        req.on("error", reject);
        req.end();
    });
}
