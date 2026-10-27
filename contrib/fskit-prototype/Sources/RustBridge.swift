// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
import Foundation
@preconcurrency import LoreFSKitBridge

struct RustBridgeError: Error, Sendable {
    let status: Int32
    var posix: POSIXError {
        let code: POSIXErrorCode
        switch status {
        case 2: code = .ENOENT
        case 3: code = .EACCES
        case 4: code = .ENOTDIR
        case 5: code = .EISDIR
        case 6: code = .EINVAL
        case 7: code = .EBADF
        case 8: code = .ESTALE
        case 9: code = .EROFS
        case 10: code = .ENOTSUP
        case 11: code = .ENOATTR
        case 12: code = .ERANGE
        case 14: code = .ENXIO
        case 15: code = .ENOSPC
        case 16: code = .ECANCELED
        case 18: code = .EBUSY
        default: code = .EIO
        }
        return POSIXError(code)
    }
}

struct RustEntry: Sendable {
    let inode: UInt64
    let parent: UInt64
    let size: UInt64
    let mode: UInt32
    let kind: UInt32
    init(_ entry: LvfsEntry) {
        inode = entry.inode; parent = entry.parent; size = entry.size
        mode = entry.mode; kind = entry.kind
    }
}
struct RustDirectoryEntry: Sendable {
    let entry: RustEntry
    let cookie: UInt64
    let name: Data
}
struct RustResult: @unchecked Sendable {
    let header: LvfsResult
    let payload: Data
    func directoryEntries() throws -> [RustDirectoryEntry] {
        let bytes = [UInt8](payload)
        var cursor = 0
        func number(_ width: Int) throws -> UInt64 {
            guard cursor <= bytes.count - width else { throw RustBridgeError(status: 13) }
            var value: UInt64 = 0
            for index in 0..<width { value |= UInt64(bytes[cursor + index]) << (index * 8) }
            cursor += width
            return value
        }
        var entries: [RustDirectoryEntry] = []
        while cursor < bytes.count {
            guard entries.count < 256 else { throw RustBridgeError(status: 15) }
            var entry = LvfsEntry()
            entry.inode = try number(8); entry.parent = try number(8); entry.size = try number(8)
            entry.mode = UInt32(try number(4)); entry.kind = UInt32(try number(4))
            let cookie = try number(8)
            let length = Int(try number(4))
            guard length <= 1000, cursor <= bytes.count - length else { throw RustBridgeError(status: 13) }
            let name = Data(bytes[cursor..<cursor + length]); cursor += length
            entries.append(RustDirectoryEntry(entry: RustEntry(entry), cookie: cookie, name: name))
        }
        return entries
    }
}

private final class RustCompletion: @unchecked Sendable {
    private let lock = NSLock()
    private var continuation: CheckedContinuation<RustResult, Error>?
    private var operation: UInt64?
    private var cancelled = false
    private var finished = false
    func install(_ continuation: CheckedContinuation<RustResult, Error>) {
        lock.withLock { self.continuation = continuation }
    }
    func bind(_ operation: UInt64) {
        lock.withLock {
            guard !finished else { return }
            self.operation = operation
            if cancelled { _ = lore_fskit_operation_cancel(operation) }
        }
    }
    func cancel() {
        lock.withLock {
            guard !finished else { return }
            cancelled = true
            if let operation { _ = lore_fskit_operation_cancel(operation) }
        }
    }
    func reject(_ status: Int32) {
        let continuation = lock.withLock { () -> CheckedContinuation<RustResult, Error>? in
            finished = true
            let continuation = self.continuation; self.continuation = nil
            return continuation
        }
        continuation?.resume(throwing: RustBridgeError(status: status))
    }
    func complete(_ operation: UInt64) {
        let completion = lock.withLock { () -> (CheckedContinuation<RustResult, Error>?, Result<RustResult, Error>) in
            var header = LvfsResult()
            let probe = lore_fskit_operation_result(operation, &header, nil, 0)
            let result: Result<RustResult, Error>
            if cancelled {
                result = .failure(CancellationError())
            } else if probe != 0 && probe != 12 {
                result = .failure(RustBridgeError(status: probe))
            } else if header.status != 0 {
                result = .failure(RustBridgeError(status: header.status))
            } else if header.payload_length > 1_048_576 {
                result = .failure(RustBridgeError(status: 15))
            } else {
                var data = Data(count: Int(header.payload_length))
                let status = data.withUnsafeMutableBytes { output in
                    lore_fskit_operation_result(operation, &header, output.baseAddress?.assumingMemoryBound(to: UInt8.self), UInt32(output.count))
                }
                if status == 0 {
                    let claim = lore_fskit_operation_claim(operation)
                    result = claim == 0 ? .success(RustResult(header: header, payload: data)) : .failure(RustBridgeError(status: claim))
                } else { result = .failure(RustBridgeError(status: status)) }
            }
            // Releasing an unclaimed open result closes its Rust handle.
            _ = lore_fskit_operation_release(operation)
            finished = true
            let continuation = self.continuation; self.continuation = nil
            return (continuation, result)
        }
        completion.0?.resume(with: completion.1)
    }
}

private let rustCompletion: @convention(c) (UnsafeMutableRawPointer?, UInt64) -> Void = { context, operation in
    guard let context else { return }
    Unmanaged<RustCompletion>.fromOpaque(context).takeRetainedValue().complete(operation)
}

private final class RustBootstrap: @unchecked Sendable {
    private let lock = NSLock()
    private var continuation: CheckedContinuation<RustSession, Error>?
    private var cancelled = false
    private var finished = false
    func install(_ continuation: CheckedContinuation<RustSession, Error>) -> Bool {
        lock.withLock {
            if cancelled { continuation.resume(throwing: CancellationError()); finished = true; return false }
            self.continuation = continuation; return true
        }
    }
    func cancel() {
        let continuation = lock.withLock { () -> CheckedContinuation<RustSession, Error>? in
            cancelled = true
            let result = self.continuation; self.continuation = nil
            if result != nil { finished = true }
            return result
        }
        continuation?.resume(throwing: CancellationError())
    }
    func finish(_ status: Int32, session: UInt64) {
        let continuation = lock.withLock { () -> CheckedContinuation<RustSession, Error>? in
            guard !finished else { return nil }
            finished = true
            let result = self.continuation; self.continuation = nil
            return result
        }
        guard let continuation else {
            // A cancelled bootstrap returns promptly; late successful setup is
            // cleaned up on this background queue and never becomes a volume.
            if session != 0 { _ = lore_fskit_session_close(session) }
            return
        }
        guard status == 0 else { continuation.resume(throwing: RustBridgeError(status: status)); return }
        do { continuation.resume(returning: try RustSession(retaining: session)) }
        catch { _ = lore_fskit_session_close(session); continuation.resume(throwing: error) }
    }
}

final class RustSession: @unchecked Sendable {
    private let lock = NSLock()
    private var value: UInt64
    private var closing = false
    private var closeResult: Int32?
    private var closeWaiters: [CheckedContinuation<Int32, Never>] = []
    init(retaining value: UInt64) throws {
        guard lore_fskit_abi_version() == 1, value != 0 else { throw RustBridgeError(status: 19) }
        self.value = value
    }
    static func open(configPath: String) async throws -> RustSession {
        guard lore_fskit_abi_version() == 1 else { throw RustBridgeError(status: 19) }
        guard !configPath.isEmpty, configPath.utf8.count <= 4096 else { throw RustBridgeError(status: 6) }
        let bootstrap = RustBootstrap()
        let session: RustSession = try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                guard bootstrap.install(continuation) else { return }
                DispatchQueue.global(qos: .userInitiated).async {
                    let bytes = Array(configPath.utf8)
                    var value: UInt64 = 0
                    let status = bytes.withUnsafeBufferPointer {
                        lore_fskit_session_open(1, $0.baseAddress, UInt32($0.count), &value)
                    }
                    bootstrap.finish(status, session: value)
                }
            }
        } onCancel: { bootstrap.cancel() }
        if Task.isCancelled { await session.close(); throw CancellationError() }
        return session
    }
    func perform(_ opcode: UInt32, inode: UInt64 = 1, handle: UInt64 = 0,
                 offset: UInt64 = 0, count: UInt32 = 0, name: Data = Data()) async throws -> RustResult {
        guard name.count <= 1000 else { throw RustBridgeError(status: 6) }
        let session = try lock.withLock { () throws -> UInt64 in
            guard value != 0 else { throw RustBridgeError(status: 14) }; return value
        }
        let completion = RustCompletion()
        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                completion.install(continuation)
                let context = Unmanaged.passRetained(completion).toOpaque()
                var operation: UInt64 = 0
                let status = name.withUnsafeBytes { bytes in
                    var request = LvfsRequest()
                    request.opcode = opcode; request.inode = inode; request.handle = handle
                    request.offset = offset; request.count = count; request.name_length = UInt32(bytes.count)
                    request.name = bytes.baseAddress?.assumingMemoryBound(to: UInt8.self)
                    return lore_fskit_submit(session, &request, rustCompletion, context, &operation)
                }
                if status == 0 { completion.bind(operation) }
                else {
                    // Rejected submissions never invoke the callback.
                    Unmanaged<RustCompletion>.fromOpaque(context).release()
                    completion.reject(status)
                }
            }
        } onCancel: { completion.cancel() }
    }
    func releaseHandle(_ handle: UInt64) {
        let session = lock.withLock { value }
        if session != 0 { _ = lore_fskit_handle_release(session, handle) }
    }
    @discardableResult
    func close() async -> Int32 {
        await withCheckedContinuation { continuation in
            let session = lock.withLock { () -> UInt64? in
                if let closeResult { continuation.resume(returning: closeResult); return nil }
                closeWaiters.append(continuation)
                guard !closing else { return nil }
                closing = true
                let session = value; value = 0; return session
            }
            guard let session else { return }
            DispatchQueue.global(qos: .utility).async {
                let status = lore_fskit_session_close(session)
                let waiters = self.lock.withLock { () -> [CheckedContinuation<Int32, Never>] in
                    self.closeResult = status
                    let waiters = self.closeWaiters; self.closeWaiters.removeAll()
                    return waiters
                }
                for waiter in waiters { waiter.resume(returning: status) }
            }
        }
    }
    deinit {
        let session = value
        if session != 0 {
            DispatchQueue.global(qos: .utility).async { _ = lore_fskit_session_close(session) }
        }
    }
}
