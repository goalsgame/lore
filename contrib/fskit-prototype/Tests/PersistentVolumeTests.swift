// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
import Foundation
import FSKit

final class FailureSwitch: @unchecked Sendable {
    private let lock = NSLock()
    private var enabled = false
    func enable() { lock.withLock { enabled = true } }
    func check(_ point: PersistentMarker.Point) throws {
        if case .generationSync = point, lock.withLock({ enabled }) { throw POSIXError(.EIO) }
    }
}

@main
struct PersistentVolumeTests {
    static func main() async throws {
        let parent = FileManager.default.temporaryDirectory.appendingPathComponent("lore-volume-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: parent, withIntermediateDirectories: false,
                                                attributes: [.posixPermissions: 0o700])
        defer { try? FileManager.default.removeItem(at: parent) }
        let run = UUID()
        let (worker, initial) = try await PersistentMarkerWorker.open(parent: parent, run: run, create: true)
        let volume = ProbeVolume(cachePolicy: "none", persistent: (worker, initial))
        let item = ProbeItem(3)
        _ = try await volume.write(contents: Data("durable".utf8), to: item, at: 0)
        async let first = volume.write(contents: Data("AA".utf8), to: item, at: 0)
        async let second = volume.write(contents: Data("BB".utf8), to: item, at: 2)
        _ = try await (first, second)
        await volume.unmount()
        let (reopened, image) = try await PersistentMarkerWorker.open(parent: parent, run: run, create: false)
        guard image.bytes == Data("AABBble\n".utf8) else { fatalError("acknowledged bytes were not recovered") }
        await reopened.closeAndDrain()
        let failures = FailureSwitch()
        let faultRun = UUID()
        let store = try PersistentMarker.open(parent: parent, run: faultRun, create: true,
                                               hook: { try failures.check($0) })
        let faultInitial = try store.publish(Data("initial".utf8))
        let faulty = ProbeVolume(cachePolicy: "none", persistent: (PersistentMarkerWorker(store), faultInitial))
        failures.enable()
        do {
            _ = try await faulty.write(contents: Data("failed".utf8), to: ProbeItem(3), at: 0)
            fatalError("failed persistence was acknowledged")
        } catch let error as POSIXError where error.code == .EIO {}
        await faulty.unmount()
        print("Persistent FSKit write callback recovered acknowledged bytes")
    }
}
