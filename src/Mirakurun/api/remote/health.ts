/*
 * Copyright 2026 nyanz00
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
 * Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */
import { Operation } from "express-openapi";
import * as api from "../../api";
import manager from "../../remote/RemoteManager";

export const get: Operation = async (req, res) => api.responseJSON(res, await manager.health(req.ip));
get.apiDoc = {
    tags: ["remote"], operationId: "getRemoteHealth",
    responses: { 200: { description: "Peer health without tuning", schema: { $ref: "#/definitions/RemoteHealth" } } }
};
