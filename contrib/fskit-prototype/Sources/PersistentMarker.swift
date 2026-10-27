// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
import CryptoKit
import Darwin
import Foundation

/// A standalone diagnostic store. A disk pointer is a recovery fact, not proof
/// that its generation was acknowledged to a caller before a crash.
final class PersistentMarker: @unchecked Sendable {
    static let capacity = 1024 * 1024
    enum Failure: Error { case invalid, busy, reconciliationRequired, limit }
    enum Point: Sendable { case admissionWrite, admissionSync, admissionDirectory, generationWrite, generationSync, generationDirectory, pointerWrite, pointerSync, replace, pointerDirectory, removeOld, reclaimDirectory, clearPending, completionDirectory }
    struct Image: Equatable, Sendable { let sequence: UInt64; let bytes: Data }
    private let parentDirectory: Int32
    private let directory: Int32
    private let owner: Int32
    let run: UUID
    private let mutex = NSLock()
    private var image: Image?
    private var previous: UInt64?
    private var lastAcknowledged: Image?
    private var poisoned = false
    private let hook: @Sendable (Point) throws -> Void
    private let writeLimit: Int

    private init(parentDirectory: Int32, directory: Int32, owner: Int32, run: UUID, image: Image?, previous: UInt64?,
                 writeLimit: Int, hook: @escaping @Sendable (Point) throws -> Void) {
        self.parentDirectory = parentDirectory; self.directory = directory; self.owner = owner; self.run = run
        self.image = image; self.previous = previous; self.writeLimit = writeLimit; self.hook = hook
    }
    deinit { _ = flock(owner, LOCK_UN); _ = Darwin.close(owner); _ = Darwin.close(directory); _ = Darwin.close(parentDirectory) }
    private static func system(_ result: Int32) throws {
        if result < 0 { throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO) }
    }
    private static func privateFile(_ fd: Int32, directory: Bool) throws {
        var info = stat(); try system(fstat(fd, &info))
        guard info.st_uid == getuid(), info.st_mode & 0o077 == 0,
              info.st_mode & S_IFMT == (directory ? S_IFDIR : S_IFREG),
              directory || info.st_nlink == 1 else { throw Failure.invalid }
    }
    static func open(parent: URL, run: UUID, create: Bool, writeLimit: Int = Int.max,
                     hook: @escaping @Sendable (Point) throws -> Void = { _ in }) throws -> PersistentMarker {
        guard writeLimit > 0 else { throw Failure.invalid }
        let parentFD = Darwin.open(parent.path, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC)
        guard parentFD >= 0 else { throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO) }
        defer { Darwin.close(parentFD) }
        try privateFile(parentFD, directory: true)
        if create { try system(mkdirat(parentFD, run.uuidString, 0o700)); try system(fsync(parentFD)) }
        let directory = openat(parentFD, run.uuidString, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC)
        guard directory >= 0 else { throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO) }
        var owner: Int32 = -1
        var transferred = false
        defer { if !transferred { if owner >= 0 { Darwin.close(owner) }; Darwin.close(directory) } }
        try privateFile(directory, directory: true)
        owner = openat(directory, "owner.lock", O_RDWR | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC | (create ? O_CREAT | O_EXCL : 0), 0o600)
        guard owner >= 0 else { throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO) }
        try privateFile(owner, directory: false)
        guard flock(owner, LOCK_EX | LOCK_NB) == 0 else { throw Failure.busy }
        if create { try system(fsync(owner)); try system(fsync(directory)) }
        let retainedParent = fcntl(parentFD, F_DUPFD_CLOEXEC, 0)
        guard retainedParent >= 0 else { throw Failure.invalid }
        let store = PersistentMarker(parentDirectory: retainedParent, directory: directory, owner: owner, run: run, image: nil,
                                     previous: nil, writeLimit: writeLimit, hook: hook)
        transferred = true
        try store.verifyOwner()
        let names = try store.inventory()
        if create { guard names == ["owner.lock"] else { throw Failure.invalid } }
        else {
            guard names.contains("current"), !names.contains("pointer.pending"), !names.contains("mutation.pending") else { throw Failure.reconciliationRequired }
            let pointer = try store.read("current", maximum: 92)
            let pair = try store.decodePointer(pointer)
            let current = try store.decodeGeneration(store.read(Self.name(pair.0), maximum: Self.capacity + 92), sequence: pair.0)
            var expected: Set<String> = ["owner.lock", "current", Self.name(pair.0)]
            if let old = pair.1 {
                _ = try store.decodeGeneration(store.read(Self.name(old), maximum: Self.capacity + 92), sequence: old)
                expected.insert(Self.name(old))
            }
            guard names == expected else { throw Failure.reconciliationRequired }
            store.image = current; store.previous = pair.1
        }
        try store.verifyOwner()
        return store
    }
    private func verifyOwner() throws {
        try Self.privateFile(parentDirectory, directory: true)
        try Self.privateFile(directory, directory: true)
        try Self.privateFile(owner, directory: false)
        var heldDirectory = stat(), namedDirectory = stat(), heldOwner = stat(), namedOwner = stat()
        try Self.system(fstat(directory, &heldDirectory))
        try Self.system(fstatat(parentDirectory, run.uuidString, &namedDirectory, AT_SYMLINK_NOFOLLOW))
        try Self.system(fstat(owner, &heldOwner))
        try Self.system(fstatat(directory, "owner.lock", &namedOwner, AT_SYMLINK_NOFOLLOW))
        guard heldDirectory.st_dev == namedDirectory.st_dev, heldDirectory.st_ino == namedDirectory.st_ino,
              namedDirectory.st_mode & S_IFMT == S_IFDIR,
              heldOwner.st_dev == namedOwner.st_dev, heldOwner.st_ino == namedOwner.st_ino,
              namedOwner.st_mode & S_IFMT == S_IFREG else { throw Failure.invalid }
    }
    func acknowledged() -> Image? { mutex.withLock { lastAcknowledged } }
    /// Restored bytes are disk facts; they do not establish a pre-crash callback acknowledgment.
    func currentImage() -> Image? { mutex.withLock { image } }
    private static func name(_ sequence: UInt64) -> String { "generation-\(sequence)" }
    private static func append(_ value: UInt64, to data: inout Data) {
        for shift in stride(from: 0, to: 64, by: 8) { data.append(UInt8(truncatingIfNeeded: value >> shift)) }
    }
    private static func integer(_ bytes: Data.SubSequence) -> UInt64 {
        bytes.enumerated().reduce(0) { $0 | UInt64($1.element) << ($1.offset * 8) }
    }
    private func encode(_ magic: String, sequence: UInt64, tail: Data) -> Data {
        var data = Data(magic.utf8); data.append(contentsOf: run.uuidString.utf8)
        Self.append(sequence, to: &data); data.append(tail)
        data.append(contentsOf: SHA256.hash(data: data)); return data
    }
    private func valid(_ data: Data, magic: String) throws -> Data {
        guard data.count >= 84, data.prefix(8) == Data(magic.utf8),
              data[8..<44] == Data(run.uuidString.utf8),
              data.suffix(32) == Data(SHA256.hash(data: data.dropLast(32))) else { throw Failure.invalid }
        return Data(data.dropLast(32))
    }
    private func decodePointer(_ data: Data) throws -> (UInt64, UInt64?) {
        let body = try valid(data, magic: "LOREPTR1")
        guard body.count == 60 else { throw Failure.invalid }
        let current = Self.integer(body[44..<52]), old = Self.integer(body[52..<60])
        guard current > 0, old == current - 1 else { throw Failure.invalid }
        return (current, old == 0 ? nil : old)
    }
    private func decodeGeneration(_ data: Data, sequence: UInt64) throws -> Image {
        let body = try valid(data, magic: "LOREGEN1")
        guard body.count >= 60, Self.integer(body[44..<52]) == sequence,
              Self.integer(body[52..<60]) == body.count - 60, body.count - 60 <= Self.capacity else { throw Failure.invalid }
        return Image(sequence: sequence, bytes: Data(body.dropFirst(60)))
    }
    private func read(_ name: String, maximum: Int) throws -> Data {
        let fd = openat(directory, name, O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC)
        guard fd >= 0 else { throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO) }
        defer { Darwin.close(fd) }; try Self.privateFile(fd, directory: false)
        var info = stat(); try Self.system(fstat(fd, &info))
        guard info.st_size >= 0, info.st_size <= maximum else { throw Failure.limit }
        var data = Data(count: Int(info.st_size))
        try data.withUnsafeMutableBytes { output in
            var offset = 0
            while offset < output.count {
                let count = Darwin.read(fd, output.baseAddress!.advanced(by: offset), output.count - offset)
                if count < 0 && errno == EINTR { continue }
                guard count > 0 else { throw Failure.invalid }; offset += count
            }
        }
        var extra: UInt8 = 0
        guard Darwin.read(fd, &extra, 1) == 0 else { throw Failure.invalid }
        return data
    }
    private func inventory() throws -> Set<String> {
        // Independent stream offset, bounded names/count; never collect an arbitrary directory.
        let fd = openat(directory, ".", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC)
        guard fd >= 0 else { throw Failure.invalid }
        guard let stream = fdopendir(fd) else { Darwin.close(fd); throw Failure.invalid }
        defer { closedir(stream) }
        var names: Set<String> = []
        while true {
            errno = 0
            guard let entry = readdir(stream) else {
                if errno != 0 { throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO) }; break
            }
            let name = try withUnsafeBytes(of: entry.pointee.d_name) { raw -> String in
                guard let end = raw.firstIndex(of: 0), end <= 64,
                      let name = String(bytes: raw.prefix(end), encoding: .utf8) else { throw Failure.invalid }
                return name
            }
            if name == "." || name == ".." { continue }
            guard names.count < 7 else { throw Failure.limit }
            names.insert(name)
        }
        return names
    }
    private func write(_ data: Data, name: String, writePoint: Point, syncPoint: Point) throws {
        let fd = openat(directory, name, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0o600)
        guard fd >= 0 else { throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO) }
        defer { Darwin.close(fd) }
        try hook(writePoint)
        try data.withUnsafeBytes { bytes in
            var offset = 0
            while offset < bytes.count {
                let count = Darwin.write(fd, bytes.baseAddress!.advanced(by: offset), min(writeLimit, bytes.count - offset))
                if count < 0 && errno == EINTR { continue }
                guard count > 0 else { throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO) }; offset += count
            }
        }
        try hook(syncPoint); try Self.system(fsync(fd)); try Self.system(fcntl(fd, F_FULLFSYNC))
    }
    @discardableResult func publish(_ bytes: Data) throws -> Image {
        try mutex.withLock { try publishLocked(bytes) }
    }
    private func publishLocked(_ bytes: Data) throws -> Image {
        guard !poisoned else { throw Failure.reconciliationRequired }
        guard bytes.count <= Self.capacity, (image?.sequence ?? 0) < UInt64.max else { throw Failure.limit }
        do {
            try verifyOwner()
            var expected: Set<String> = ["owner.lock"]
            if let image { expected.formUnion(["current", Self.name(image.sequence)]) }
            if let previous { expected.insert(Self.name(previous)) }
            guard try inventory() == expected else { throw Failure.reconciliationRequired }
        } catch { poisoned = true; throw error }
        let next = (image?.sequence ?? 0) + 1
        var length = Data(); Self.append(UInt64(bytes.count), to: &length); length.append(bytes)
        let record = encode("LOREGEN1", sequence: next, tail: length)
        var prior = Data(); Self.append(image?.sequence ?? 0, to: &prior)
        let pointer = encode("LOREPTR1", sequence: next, tail: prior)
        do {
            let marker = encode("LORETXN1", sequence: next, tail: prior)
            try write(marker, name: "mutation.pending", writePoint: .admissionWrite, syncPoint: .admissionSync)
            try hook(.admissionDirectory); try Self.system(fsync(directory))
            try write(record, name: Self.name(next), writePoint: .generationWrite, syncPoint: .generationSync)
            try hook(.generationDirectory); try Self.system(fsync(directory))
            try write(pointer, name: "pointer.pending", writePoint: .pointerWrite, syncPoint: .pointerSync)
            try hook(.replace); try Self.system(renameat(directory, "pointer.pending", directory, "current"))
            try hook(.pointerDirectory); try Self.system(fsync(directory))
            if let old = previous {
                try hook(.removeOld); try Self.system(unlinkat(directory, Self.name(old), 0))
                try hook(.reclaimDirectory); try Self.system(fsync(directory))
            }
            try hook(.clearPending); try Self.system(unlinkat(directory, "mutation.pending", 0))
            try hook(.completionDirectory); try Self.system(fsync(directory))
            let published = Image(sequence: next, bytes: bytes)
            previous = image?.sequence; image = published; lastAcknowledged = published
            return published
        } catch { poisoned = true; throw error }
    }
    func write(_ contents: Data, at offset: Int64) throws -> Image {
        try mutex.withLock {
            guard offset >= 0, offset <= Self.capacity, contents.count <= Self.capacity - Int(offset),
                  var bytes = image?.bytes else { throw Failure.limit }
            let end = Int(offset) + contents.count
            if end > bytes.count { bytes.count = end }
            bytes.replaceSubrange(Int(offset)..<end, with: contents)
            return try publishLocked(bytes)
        }
    }
    func truncate(_ size: UInt64) throws -> Image {
        try mutex.withLock {
            guard size <= Self.capacity, var bytes = image?.bytes else { throw Failure.limit }
            bytes.count = Int(size)
            return try publishLocked(bytes)
        }
    }
    func synchronize() throws -> Image {
        try mutex.withLock {
            guard !poisoned, let image else { throw Failure.reconciliationRequired }
            do {
                try verifyOwner()
                let fd = openat(directory, Self.name(image.sequence), O_RDWR | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC)
                guard fd >= 0 else { throw Failure.invalid }
                defer { Darwin.close(fd) }
                try Self.privateFile(fd, directory: false)
                try Self.system(fsync(fd)); try Self.system(fcntl(fd, F_FULLFSYNC))
                try Self.system(fsync(directory))
                return image
            } catch { poisoned = true; throw error }
        }
    }

}

/// Accepted jobs own the engine until actual IO finishes. Cancelling an awaiting
/// task does not cancel its dispatch job or make the exclusive claim reusable.
final class PersistentMarkerWorker: @unchecked Sendable {
    private let queue = DispatchQueue(label: "org.example.lore.marker-io")
    private let gate = NSLock()
    private var engine: PersistentMarker?
    private var count = 0
    private var closed = false
    init(_ engine: PersistentMarker) { self.engine = engine }
    static func open(parent: URL, run: UUID, create: Bool, createParent: Bool = false) async throws -> (PersistentMarkerWorker, PersistentMarker.Image) {
        try await withCheckedThrowingContinuation { continuation in
            DispatchQueue.global(qos: .utility).async {
                let result = Result {
                    if createParent {
                        try FileManager.default.createDirectory(at: parent, withIntermediateDirectories: true,
                                                                attributes: [.posixPermissions: 0o700])
                    }
                    let engine = try PersistentMarker.open(parent: parent, run: run, create: create)
                    let image: PersistentMarker.Image
                    if create { image = try engine.publish(Data("edit me\n".utf8)) }
                    else {
                        guard let restored = engine.currentImage() else { throw PersistentMarker.Failure.invalid }
                        image = restored
                    }
                    return (PersistentMarkerWorker(engine), image)
                }
                continuation.resume(with: result)
            }
        }
    }
    private enum Operation: Sendable { case replace(Data), write(Data, Int64), truncate(UInt64), synchronize }
    private final class Job: @unchecked Sendable {
        var engine: PersistentMarker?
        var operation: Operation?
        init(_ engine: PersistentMarker, _ operation: Operation) { self.engine = engine; self.operation = operation }
        func perform() -> Result<PersistentMarker.Image, Error> {
            let result = Result {
                switch operation! {
                case .replace(let bytes): return try engine!.publish(bytes)
                case .write(let bytes, let offset): return try engine!.write(bytes, at: offset)
                case .truncate(let size): return try engine!.truncate(size)
                case .synchronize: return try engine!.synchronize()
                }
            }
            operation = nil; engine = nil
            return result
        }
    }
    func publish(_ bytes: Data) async throws -> PersistentMarker.Image {
        guard bytes.count <= PersistentMarker.capacity else { throw PersistentMarker.Failure.limit }
        return try await submit(.replace(bytes))
    }
    func write(_ bytes: Data, at offset: Int64) async throws -> PersistentMarker.Image {
        guard offset >= 0, offset <= PersistentMarker.capacity,
              bytes.count <= PersistentMarker.capacity - Int(offset) else { throw PersistentMarker.Failure.limit }
        return try await submit(.write(bytes, offset))
    }
    func truncate(_ size: UInt64) async throws -> PersistentMarker.Image {
        guard size <= PersistentMarker.capacity else { throw PersistentMarker.Failure.limit }
        return try await submit(.truncate(size))
    }
    func synchronize() async throws -> PersistentMarker.Image { try await submit(.synchronize) }
    private func submit(_ operation: Operation) async throws -> PersistentMarker.Image {
        try await withCheckedThrowingContinuation { continuation in
            let accepted = gate.withLock { () -> Bool in
                guard !closed, count < 4, let engine else { return false }
                let job = Job(engine, operation)
                count += 1
                // Enqueue under admission so a concurrent drain cannot overtake it.
                queue.async { [self] in
                    let result = job.perform()
                    gate.withLock { count -= 1 }
                    continuation.resume(with: result)
                }
                return true
            }
            if !accepted { continuation.resume(throwing: PersistentMarker.Failure.busy) }
        }
    }
    func closeAndDrain() async {
        await withCheckedContinuation { continuation in
            gate.withLock {
                closed = true
                queue.async { [self] in
                    var retired = gate.withLock { () -> PersistentMarker? in
                        let retired = engine; engine = nil; return retired
                    }
                    withExtendedLifetime(retired) {}
                    retired = nil
                    continuation.resume()
                }
            }
        }
    }
}

/// The load record owns actual setup even if the FSKit loading task is cancelled.
/// Unload waits for that same setup result and drains its exact worker.
final class PersistentMarkerLoad: @unchecked Sendable {
    private let task: Task<(PersistentMarkerWorker, PersistentMarker.Image), Error>
    init(parent: URL, run: UUID, create: Bool) {
        task = Task.detached {
            try await PersistentMarkerWorker.open(parent: parent, run: run, create: create, createParent: create)
        }
    }
    func value() async throws -> (PersistentMarkerWorker, PersistentMarker.Image) { try await task.value }
    func close() async {
        if let (worker, _) = try? await task.value { await worker.closeAndDrain() }
    }
}
