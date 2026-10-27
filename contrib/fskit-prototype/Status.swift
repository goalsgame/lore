// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
import FSKit
import Foundation

@main
struct ProbeStatus {
    static func main() async throws {
        if CommandLine.arguments.contains("--settings") {
            guard FSClient.shared.openFileSystemExtensionsSettings() else { exit(1) }
            return
        }
        let modules = try await FSClient.shared.installedExtensions
        let matching = modules.filter { $0.bundleIdentifier == "org.example.lore.fskit-probe.extension" }
        if matching.isEmpty {
            print("Lore FSKit prototype is not visible to this client (check registration and client signing)")
            exit(1)
        }
        for module in matching {
            print("\(module.bundleIdentifier) enabled=\(module.isEnabled) path=\(module.url.path)")
        }
        if !matching.contains(where: { $0.isEnabled }) { exit(2) }
    }
}
