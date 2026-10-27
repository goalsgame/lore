// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
import CryptoKit
import Foundation

/// Small, bounded fixtures for exercising the OS adapter, independent of Lore stores.
struct ProbeModel {
    enum Kind { case file, directory, symlink }
    struct Entry {
        let id: UInt64
        let kind: Kind
        var bytes: Data
        var clean = true
        var generation: UInt64 = 1

        var digest: Data? {
            kind == .file && clean ? Data(SHA256.hash(data: bytes)) : nil
        }
    }

    static let capacity = 1024 * 1024
    var revision = 0
    var reads: UInt64 = 0
    var digestRequests: UInt64 = 0
    var entries: [UInt64: Entry] = [
        1: Entry(id: 1, kind: .directory, bytes: Data()),
        2: Entry(id: 2, kind: .file, bytes: Data("hello lore\n".utf8)),
        3: Entry(id: 3, kind: .file, bytes: Data("edit me\n".utf8)),
        4: Entry(id: 4, kind: .file, bytes: Data("removed\n".utf8)),
        5: Entry(id: 5, kind: .file, bytes: Data("added\n".utf8)),
        6: Entry(id: 6, kind: .file, bytes: Data("file\n".utf8)),
        7: Entry(id: 7, kind: .directory, bytes: Data()),
        8: Entry(id: 8, kind: .symlink, bytes: Data("value.txt".utf8)),
    ]

    var names: [String: UInt64] {
        var names: [String: UInt64] = ["value.txt": 2, "edit.txt": 3, "target": 8]
        names[revision == 0 ? "gone.txt" : "appeared.txt"] = revision == 0 ? 4 : 5
        names["kind"] = revision == 0 ? 6 : 7
        return names
    }

    mutating func switchRevision(_ next: Int) throws {
        guard next == 0 || next == 1 else { throw POSIXError(.EINVAL) }
        revision = next
        entries[2]!.bytes = Data((next == 0 ? "hello lore\n" : "HELLO lore\n").utf8)
        entries[8]!.bytes = Data((next == 0 ? "value.txt" : "appeared.txt").utf8)
        for id: UInt64 in [1, 2, 8] { entries[id]!.generation += 1 }
    }

    mutating func write(_ data: Data, id: UInt64, offset: Int64) throws {
        guard id == 3 else { throw POSIXError(.EROFS) }
        guard offset >= 0, offset <= Int64(Self.capacity),
              data.count <= Self.capacity - Int(offset) else { throw POSIXError(.EFBIG) }
        var entry = entries[id]!
        // Digest availability changes in the same transaction as the bytes.
        entry.clean = false
        entry.generation += 1
        let end = Int(offset) + data.count
        if end > entry.bytes.count { entry.bytes.count = end }
        entry.bytes.replaceSubrange(Int(offset)..<end, with: data)
        entries[id] = entry
    }

    mutating func truncate(id: UInt64, size: UInt64) throws {
        guard id == 3 else { throw POSIXError(.EROFS) }
        guard size <= UInt64(Self.capacity) else { throw POSIXError(.EFBIG) }
        entries[id]!.clean = false
        entries[id]!.generation += 1
        entries[id]!.bytes.count = Int(size)
    }
}
