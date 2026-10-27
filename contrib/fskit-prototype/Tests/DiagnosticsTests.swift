// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
import Foundation

@main
struct DiagnosticsTests {
    static func main() throws {
        let valid: [String: Any] = ["token": "fixture", "item": 1, "mode": 0,
                                  "coherency": 0, "action": 2, "delay_ms": 0]
        func decode(_ fields: [String: Any]) throws -> ProbeCacheCommand {
            try ProbeCacheCommand(data: JSONSerialization.data(withJSONObject: fields))
        }
        _ = try decode(valid)
        for item in [0, 3, 9] {
            var fields = valid; fields["item"] = item
            do { _ = try decode(fields); preconditionFailure("Dirty or unknown items must not be invalidated") }
            catch let error as POSIXError { precondition(error.code == .EINVAL) }
        }
        for (field, value) in [("mode", 3), ("coherency", 4), ("action", 5), ("delay_ms", 251)] {
            var fields = valid; fields[field] = value
            do { _ = try decode(fields); preconditionFailure("Invalid kernel controls must be refused") }
            catch let error as POSIXError { precondition(error.code == .EINVAL) }
        }
        var revoke = valid; revoke["action"] = 4
        do { _ = try decode(revoke); preconditionFailure("Revoke must require explicit opt-in") }
        catch let error as POSIXError { precondition(error.code == .EINVAL) }
        revoke["allow_revoke"] = true
        _ = try decode(revoke)
        let underlying = NSError(domain: "FixtureUnderlying", code: 22)
        let result = try decode(valid).result(NSError(domain: "FixtureError", code: 1,
            userInfo: [NSUnderlyingErrorKey: underlying]))
        precondition(result["underlying_domain"] as? String == "FixtureUnderlying")
        precondition(result["underlying_code"] as? Int == 22)
        print("Diagnostic controls refuse dirty edit.txt, invalid kernel values and implicit revoke; no FSKit runtime invoked")
    }
}
