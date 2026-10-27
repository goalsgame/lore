// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
import FSKit
import Foundation

/// Explicit diagnostics operate only on immutable fixture items, never dirty edit.txt.
struct ProbeCacheCommand: Sendable {
    let token: String
    let item: UInt64
    let mode: FSVolume.DataCacheMode
    let coherency: FSVolume.KernelCacheCoherencyType
    let action: FSVolume.KernelCacheCoherencyAction
    let delayMS: Int

    init(data: Data) throws {
        guard let object = try JSONSerialization.jsonObject(with: data) as? [String: Any],
              Set(object.keys).isSubset(of: ["token", "item", "mode", "coherency", "action", "delay_ms", "allow_revoke"]),
              let token = object["token"] as? String, !token.isEmpty, token.utf8.count <= 64,
              let item = object["item"] as? UInt64, [UInt64(1), 2, 7, 8].contains(item),
              let mode = object["mode"] as? Int, (0...2).contains(mode), let cacheMode = FSVolume.DataCacheMode(rawValue: mode),
              let coherency = object["coherency"] as? Int, (0...3).contains(coherency),
              let coherencyType = FSVolume.KernelCacheCoherencyType(rawValue: coherency),
              let action = object["action"] as? Int, (0...4).contains(action),
              let cacheAction = FSVolume.KernelCacheCoherencyAction(rawValue: action),
              let delay = object["delay_ms"] as? Int, (0...250).contains(delay),
              action != FSVolume.KernelCacheCoherencyAction.revoke.rawValue || object["allow_revoke"] as? Bool == true
        else { throw POSIXError(.EINVAL) }
        self.token = token; self.item = item; self.mode = cacheMode
        self.coherency = coherencyType; self.action = cacheAction; self.delayMS = delay
    }
    func result(_ error: Error?) -> [String: Any] {
        var value: [String: Any] = ["token": token, "status": error == nil ? "ok" : "error",
            "item": item, "mode": mode.rawValue, "coherency": coherency.rawValue,
            "action": action.rawValue, "delay_ms": delayMS]
        if let error = error as NSError? {
            value["domain"] = error.domain; value["code"] = error.code
            value["description"] = String(error.localizedDescription.prefix(384))
            if let underlying = error.userInfo[NSUnderlyingErrorKey] as? NSError {
                value["underlying_domain"] = underlying.domain; value["underlying_code"] = underlying.code
            }
        }
        return value
    }
}
