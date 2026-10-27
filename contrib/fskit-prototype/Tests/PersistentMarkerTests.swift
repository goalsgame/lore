// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
import Darwin
import Foundation

private final class Faults: @unchecked Sendable {
    let lock = NSLock()
    var point: PersistentMarker.Point?
    func check(_ point: PersistentMarker.Point) throws {
        if lock.withLock({ self.point == point }) { throw POSIXError(.EIO) }
    }
}
private final class Pause: @unchecked Sendable {
    let reached = DispatchSemaphore(value: 0)
    let release = DispatchSemaphore(value: 0)
    func check(_ point: PersistentMarker.Point) {
        if point == .generationWrite { reached.signal(); release.wait() }
    }
}
@main
struct PersistentMarkerTests {
    static func root() throws -> URL {
        let url = FileManager.default.temporaryDirectory.appendingPathComponent("lore-marker-tests-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: url, withIntermediateDirectories: false,
                                                attributes: [.posixPermissions: 0o700])
        return url
    }
    static func fails(_ operation: () throws -> Void) {
        do { try operation(); preconditionFailure("Expected refusal") } catch {}
    }
    static func main() async throws {
        let parent = try root()
        defer { try? FileManager.default.removeItem(at: parent) }
        try roundtrip(parent)
        try failures(parent)
        try ownership(parent)
        try replacements(parent)
        try await lifetime(parent)
        print("Persistent marker: exact-capacity/short-write roundtrip, publication faults, bounded recovery, exclusive owner, and actual-IO cancellation/drain passed")
    }
    static func roundtrip(_ parent: URL) throws {
        let run = UUID()
        var store: PersistentMarker? = try .open(parent: parent, run: run, create: true, writeLimit: 3)
        let first = try store!.publish(Data("one".utf8))
        precondition(first.sequence == 1)
        let second = try store!.publish(Data("two".utf8))
        precondition(second.sequence == 2)
        let maximum = Data(repeating: 0xA7, count: PersistentMarker.capacity)
        let third = try store!.publish(maximum)
        precondition(third.sequence == 3 && third.bytes == maximum)
        precondition(store!.acknowledged() == third)
        let names = try FileManager.default.contentsOfDirectory(atPath: parent.appendingPathComponent(run.uuidString).path)
        precondition(Set(names) == ["owner.lock", "current", "generation-2", "generation-3"])
        fails { _ = try store!.publish(Data(count: PersistentMarker.capacity + 1)) }
        precondition(store!.acknowledged() == third)
        store = nil
        let restored = try PersistentMarker.open(parent: parent, run: run, create: false)
        precondition(restored.currentImage() == third)
        precondition(restored.acknowledged() == nil, "Restored disk pointer must not invent a pre-crash acknowledgment")
    }
    static func failures(_ parent: URL) throws {
        let points: [PersistentMarker.Point] = [.admissionWrite, .admissionSync, .admissionDirectory,
            .generationWrite, .generationSync, .generationDirectory, .pointerWrite, .pointerSync,
            .replace, .pointerDirectory, .removeOld, .reclaimDirectory, .clearPending, .completionDirectory]
        for point in points {
            let run = UUID(), faults = Faults()
            var store: PersistentMarker? = try .open(parent: parent, run: run, create: true, hook: { try faults.check($0) })
            _ = try store!.publish(Data("first".utf8))
            let before = try store!.publish(Data("second".utf8))
            let directory = parent.appendingPathComponent(run.uuidString)
            let oldPointer = try Data(contentsOf: directory.appendingPathComponent("current"))
            let oldRecord = try Data(contentsOf: directory.appendingPathComponent("generation-2"))
            faults.lock.withLock { faults.point = point }
            fails { _ = try store!.publish(Data("third".utf8)) }
            precondition(store!.acknowledged() == before)
            let retainedRecord = try Data(contentsOf: directory.appendingPathComponent("generation-2"))
            precondition(retainedRecord == oldRecord)
            let afterPointer = try Data(contentsOf: directory.appendingPathComponent("current"))
            switch point {
            case .pointerDirectory, .removeOld, .reclaimDirectory, .clearPending, .completionDirectory:
                precondition(afterPointer != oldPointer)
            default:
                precondition(afterPointer == oldPointer)
            }
            faults.lock.withLock { faults.point = nil }
            fails { _ = try store!.publish(Data("must refuse".utf8)) }
            let generations = try FileManager.default.contentsOfDirectory(atPath: directory.path).filter { $0.hasPrefix("generation-") }
            precondition(generations.count <= 3)
            store = nil
            if point != .completionDirectory {
                fails { _ = try PersistentMarker.open(parent: parent, run: run, create: false) }
            } else {
                // Clear-dir-sync uncertainty can leave a valid new pointer. It is
                // still merely a disk fact, not an acknowledgment inferred on reopen.
                let fact = try PersistentMarker.open(parent: parent, run: run, create: false)
                precondition(fact.currentImage()?.sequence == 3 && fact.acknowledged() == nil)
            }
        }
    }
    static func ownership(_ parent: URL) throws {
        let run = UUID()
        var store: PersistentMarker? = try .open(parent: parent, run: run, create: true)
        _ = try store!.publish(Data("owned".utf8))
        fails { _ = try PersistentMarker.open(parent: parent, run: run, create: false) }
        store = nil
        let directory = parent.appendingPathComponent(run.uuidString)
        let owner = directory.appendingPathComponent("owner.lock")
        let alias = parent.appendingPathComponent("owner-alias")
        try FileManager.default.linkItem(at: owner, to: alias)
        fails { _ = try PersistentMarker.open(parent: parent, run: run, create: false) }
        try FileManager.default.removeItem(at: alias)
        let generation = directory.appendingPathComponent("generation-1")
        try FileManager.default.linkItem(at: generation, to: alias)
        fails { _ = try PersistentMarker.open(parent: parent, run: run, create: false) }
        try FileManager.default.removeItem(at: alias)
        try FileManager.default.removeItem(at: generation)
        try FileManager.default.createSymbolicLink(at: generation, withDestinationURL: owner)
        fails { _ = try PersistentMarker.open(parent: parent, run: run, create: false) }
        fails { _ = try PersistentMarker.open(parent: parent, run: UUID(), create: false) }
    }
    static func replacements(_ parent: URL) throws {
        let run = UUID()
        let store = try PersistentMarker.open(parent: parent, run: run, create: true)
        _ = try store.publish(Data("original".utf8))
        let named = parent.appendingPathComponent(run.uuidString)
        let moved = parent.appendingPathComponent(UUID().uuidString)
        try FileManager.default.moveItem(at: named, to: moved)
        try FileManager.default.createDirectory(at: named, withIntermediateDirectories: false, attributes: [.posixPermissions: 0o700])
        try FileManager.default.moveItem(at: moved.appendingPathComponent("owner.lock"), to: named.appendingPathComponent("owner.lock"))
        fails { _ = try store.publish(Data("replacement must not acquire old claim".utf8)) }
        precondition(store.acknowledged()?.sequence == 1)

        let other = UUID()
        var corrupt: PersistentMarker? = try .open(parent: parent, run: other, create: true)
        _ = try corrupt!.publish(Data("checksum".utf8))
        corrupt = nil
        let record = parent.appendingPathComponent(other.uuidString).appendingPathComponent("generation-1")
        var data = try Data(contentsOf: record)
        data[60] ^= 1
        try data.write(to: record)
        fails { _ = try PersistentMarker.open(parent: parent, run: other, create: false) }
    }
    static func lifetime(_ parent: URL) async throws {
        let run = UUID(), pause = Pause()
        var store: PersistentMarker? = try .open(parent: parent, run: run, create: true, hook: { pause.check($0) })
        weak let weakStore = store
        let worker = PersistentMarkerWorker(store!)
        store = nil
        let task = Task { try await worker.publish(Data("actual IO".utf8)) }
        precondition(pause.reached.wait(timeout: .now() + 5) == .success)
        task.cancel()
        precondition(weakStore != nil)
        fails { _ = try PersistentMarker.open(parent: parent, run: run, create: false) }
        let drained = DispatchSemaphore(value: 0)
        let closing = Task { await worker.closeAndDrain(); drained.signal() }
        precondition(drained.wait(timeout: .now() + 0.05) == .timedOut)
        pause.release.signal()
        let image = try await task.value
        precondition(image.sequence == 1)
        await closing.value
        precondition(weakStore == nil, "Drain must release actual owner, including completed job captures")
        let restored = try PersistentMarker.open(parent: parent, run: run, create: false)
        precondition(restored.currentImage() == image)
        do { _ = try await worker.publish(Data()); preconditionFailure("Closed worker admitted IO") } catch {}
    }
}
