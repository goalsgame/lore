// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
import Foundation
import LoreFSKitBridge

@main
struct BridgeTests {
    static func main() async throws {
        precondition(lore_fskit_abi_version() == 1)
        precondition(MemoryLayout<LvfsEntry>.size == 32)
        precondition(MemoryLayout<LvfsResult>.size == 56)
        var invalid: UInt64 = 0
        precondition(lore_fskit_fixture_open(99, &invalid) == 19 && invalid == 0)
        var value: UInt64 = 0
        precondition(lore_fskit_fixture_open(1, &value) == 0)
        let session = try RustSession(retaining: value)
        let named = try await session.perform(10, name: Data("FILE".utf8))
        precondition(named.payload == Data("file".utf8))
        let file = named.header.entry.inode
        let opened = try await session.perform(3, inode: file).header.value
        let read = try await session.perform(5, handle: opened, count: 7)
        precondition(read.payload == Data("payload".utf8))
        session.releaseHandle(opened)
        let digest = try await session.perform(8, inode: file)
        precondition(digest.payload == Data(repeating: 42, count: 32))
        let directory = try await session.perform(4).header.value
        let first = try await session.perform(6, handle: directory, count: 1)
        let firstEntries = try first.directoryEntries()
        precondition(firstEntries.count == 1 && firstEntries[0].name == Data("file".utf8))
        let second = try await session.perform(6, handle: directory, offset: firstEntries[0].cookie, count: 1)
        let secondEntries = try second.directoryEntries()
        precondition(secondEntries.first?.name == Data("linked".utf8))
        session.releaseHandle(directory)
        let linked = try await session.perform(2, name: Data("linked".utf8)).header.entry.inode
        let child = try await session.perform(2, inode: linked, name: Data("child".utf8)).header.entry.inode
        do { _ = try await session.perform(8, inode: child); preconditionFailure("Unknown digest must be absent") }
        catch let error as RustBridgeError { precondition(error.status == 11) }
        do { _ = try await session.perform(2, name: Data("denied".utf8)); preconditionFailure("Denied repository must stay inaccessible") }
        catch let error as RustBridgeError { precondition(error.status == 3) }
        let cancelled = Task { try await session.perform(2, name: Data("blocked".utf8)) }
        cancelled.cancel()
        do { _ = try await cancelled.value; preconditionFailure("Cancellation must complete") }
        catch is CancellationError {}
        async let firstClose = session.close()
        async let secondClose = session.close()
        let closeStatuses = await (firstClose, secondClose)
        precondition(closeStatuses.0 == 0 && closeStatuses.1 == 0)
        precondition(lore_fskit_session_close(value) == 7)
        do { _ = try await session.perform(1); preconditionFailure("Closed session must reject work") }
        catch let error as RustBridgeError { precondition(error.status == 14) }
        print("Rust/Swift shared-Core bridge contracts passed (no FSKit runtime exercised)")
    }
}
