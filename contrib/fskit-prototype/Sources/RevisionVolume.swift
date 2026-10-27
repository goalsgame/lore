// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
import FSKit
import Foundation

private final class RevisionItem: FSItem {
    let inode: UInt64
    init(_ inode: UInt64) { self.inode = inode; super.init() }
}

/// A pinned, read-only view. Repository content and authorization stay in Rust.
final class RevisionVolume: FSVolume, FSVolume.Handler, FSVolume.ReadWriteHandler,
                            FSVolume.XattrHandler, FSVolume.DataCacheHandler, @unchecked Sendable {
    private let session: RustSession
    private let lock = NSLock()
    private var items: [UInt64: RevisionItem] = [1: RevisionItem(1)]
    private var directories: [UInt64: UInt64] = [:]
    init(session: RustSession) {
        self.session = session
        super.init(volumeID: FSVolume.Identifier(uuid: UUID()), volumeName: FSFileName(string: "Lore pinned revision"))
    }
    var maximumLinkCount: Int { 1 }
    var maximumNameLength: Int { 255 }
    var maximumXattrSize: Int { 32 }
    var maximumFileSize: UInt64 { UInt64(Int64.max) }
    var restrictsOwnershipChanges: Bool { true }
    var truncatesLongNames: Bool { false }
    var isDataCacheInhibited: Bool { false }
    var supportedVolumeCapabilities: FSVolume.SupportedCapabilities {
        let capabilities = FSVolume.SupportedCapabilities()
        capabilities.supportsSymbolicLinks = false
        capabilities.caseFormat = .insensitiveCasePreserving
        return capabilities
    }
    var volumeStatistics: FSStatFSResult {
        let statistics = FSStatFSResult(fileSystemTypeName: "lorerevision")
        statistics.blockSize = 4096; statistics.ioSize = 65536
        statistics.totalBytes = 0; statistics.freeBytes = 0; statistics.availableBytes = 0
        statistics.totalFiles = lock.withLock { UInt64(items.count) }
        return statistics
    }
    private func id(_ item: FSItem) throws -> UInt64 {
        guard let item = item as? RevisionItem else { throw POSIXError(.ESTALE) }
        return item.inode
    }
    private func item(_ inode: UInt64) throws -> RevisionItem {
        try lock.withLock {
            if let existing = items[inode] { return existing }
            guard items.count < 65_536 else { throw POSIXError(.ENOSPC) }
            let item = RevisionItem(inode); items[inode] = item; return item
        }
    }
    private func call(_ opcode: UInt32, inode: UInt64 = 1, handle: UInt64 = 0,
                      offset: UInt64 = 0, count: UInt32 = 0, name: Data = Data()) async throws -> RustResult {
        do { return try await session.perform(opcode, inode: inode, handle: handle, offset: offset, count: count, name: name) }
        catch let error as RustBridgeError { throw error.posix }
        catch is CancellationError { throw POSIXError(.ECANCELED) }
    }
    private func attributes(_ entry: RustEntry) -> FSItem.Attributes {
        lock.withLock {
            let attributes = FSItem.Attributes()
            attributes.fileID = FSItem.Identifier(entry.inode)
            attributes.parentID = FSItem.Identifier(entry.parent)
            attributes.type = entry.kind == 2 ? .directory : entry.kind == 3 ? .symlink : .file
            attributes.mode = entry.mode & ~0o222
            attributes.uid = getuid(); attributes.gid = getgid(); attributes.linkCount = 1
            attributes.size = entry.size; attributes.allocSize = entry.size; attributes.flags = 0
            attributes.inhibitKernelOffloadedIO = true
            let time = timespec(tv_sec: 1_700_000_000, tv_nsec: 0)
            attributes.modifyTime = time; attributes.changeTime = time; attributes.birthTime = time
            attributes.accessTime = time; attributes.addedTime = time; attributes.backupTime = timespec(tv_sec: 0, tv_nsec: 0)
            return attributes
        }
    }
    private func checked<T>(_ value: T?) throws -> T {
        guard let value else { throw POSIXError(.EIO) }; return value
    }
    func activateVolume(options: FSTaskOptions) async throws -> FSActivateResult {
        _ = try await call(1)
        return try checked(FSActivateResult(rootItem: try item(1)))
    }
    func deactivateVolume(options: FSDeactivateOptions) async throws { try await shutdown() }
    func mount(options: FSTaskOptions) async throws {}
    func unmount() async { try? await shutdown() }
    func synchronize(flags: FSSyncFlags) async throws {}
    func shutdown() async throws {
        lock.withLock { directories.removeAll(); items.removeAll() }
        let status = await session.close()
        guard status == 0 || status == 7 else { throw RustBridgeError(status: status).posix }
    }
    func reclaimItem(_ item: FSItem) async throws {
        let inode = try id(item)
        let handle = lock.withLock { () -> UInt64? in
            if inode != 1 { items.removeValue(forKey: inode) }
            return directories.removeValue(forKey: inode)
        }
        if let handle { session.releaseHandle(handle) }
    }
    func lookupItem(named name: FSFileName, in directory: FSItem, context: FSContext) async throws -> FSLookupItemResult {
        guard name.data.count <= 255 else { throw POSIXError(.ENAMETOOLONG) }
        let result = try await call(10, inode: try id(directory), name: name.data)
        let entry = RustEntry(result.header.entry)
        guard result.payload.count <= 255 else {throw POSIXError(.ENAMETOOLONG)}
        return try checked(FSLookupItemResult(foundItem: try item(entry.inode), itemName: FSFileName(data: result.payload), itemAttributes: attributes(entry)))
    }
    func attributes(_ request: FSItem.GetAttributesRequest, of item: FSItem, context: FSContext) async throws -> FSGetAttributesResult {
        let result = try await call(1, inode: try id(item))
        return try checked(FSGetAttributesResult(attributes: attributes(RustEntry(result.header.entry))))
    }
    private func directoryHandle(_ inode: UInt64) async throws -> UInt64 {
        if let existing = lock.withLock({ directories[inode] }) { return existing }
        let opened = try await call(4, inode: inode).header.value
        do {
            let selected = try lock.withLock { () throws -> UInt64 in
                if let existing = directories[inode] { return existing }
                guard directories.count < 1024 else { throw POSIXError(.ENOSPC) }
                directories[inode] = opened; return opened
            }
            if selected != opened { session.releaseHandle(opened) }
            return selected
        } catch { session.releaseHandle(opened); throw error }
    }
    func enumerateDirectory(_ directory: FSItem, startingAt cookie: FSDirectoryCookie,
                            verifier: FSDirectoryVerifier, attributes request: FSItem.GetAttributesRequest?,
                            packer: FSDirectoryEntryPacker, context: FSContext) async throws -> FSEnumerateDirectoryResult {
        let handle = try await directoryHandle(id(directory))
        guard cookie.rawValue == 0 || verifier.rawValue == handle else { throw POSIXError(.ESTALE) }
        var offset = cookie.rawValue
        while true {
            let result = try await call(6, handle: handle, offset: offset, count: 256)
            let entries = try result.directoryEntries()
            for named in entries {
                guard named.name.count <= 255 else { throw POSIXError(.ENAMETOOLONG) }
                let entry = named.entry
                _ = try item(entry.inode)
                if !packer.packEntry(name: FSFileName(data: named.name), itemType: entry.kind == 2 ? .directory : entry.kind == 3 ? .symlink : .file,
                                     itemID: FSItem.Identifier(entry.inode), nextCookie: FSDirectoryCookie(named.cookie),
                                     attributes: request == nil ? nil : attributes(entry)) {
                    return try checked(FSEnumerateDirectoryResult(verifier: handle))
                }
                offset = named.cookie
            }
            if result.header.eof != 0 { break }
            guard !entries.isEmpty else { throw POSIXError(.EIO) }
        }
        return try checked(FSEnumerateDirectoryResult(verifier: handle))
    }
    func readSymbolicLink(_ item: FSItem, context: FSContext) async throws -> FSReadSymlinkResult {
        let inode = try id(item)
        let result = try await call(9, inode: inode)
        let entry = try await call(1, inode: inode)
        return try checked(FSReadSymlinkResult(contents: FSFileName(data: result.payload), symlinkAttributes: attributes(RustEntry(entry.header.entry))))
    }
    func read(from item: FSItem, at offset: off_t, length: Int, into buffer: FSMutableFileDataBuffer) async throws -> FSReadFileResult {
        guard offset >= 0, length >= 0 else { throw POSIXError(.EINVAL) }
        let capacity = buffer.withUnsafeMutableBytes { $0.count }
        let (_, overflow) = UInt64(offset).addingReportingOverflow(UInt64(length))
        guard length <= capacity, !overflow else { throw POSIXError(.EINVAL) }
        let inode = try id(item)
        let opened = try await call(3, inode: inode).header.value
        defer { session.releaseHandle(opened) }
        var completed = 0
        while completed < length {
            let count = min(1_048_576, length - completed)
            let result = try await call(5, handle: opened, offset: UInt64(offset) + UInt64(completed), count: UInt32(count))
            guard result.payload.count <= count else { throw POSIXError(.EIO) }
            buffer.withUnsafeMutableBytes { output in
                result.payload.withUnsafeBytes { source in
                    if source.count > 0 { output.baseAddress!.advanced(by: completed).copyMemory(from: source.baseAddress!, byteCount: source.count) }
                }
            }
            completed += result.payload.count
            if result.payload.count < count { break }
        }
        let metadata = try await call(1, inode: inode)
        return try checked(FSReadFileResult(bytesRead: completed, itemAttributes: attributes(RustEntry(metadata.header.entry))))
    }
    func open(_ item: FSItem, modes: FSVolume.OpenModes, cacheMode: FSVolume.DataCacheMode, context: FSContext) async throws -> FSOpenItemResult {
        guard !modes.contains(.write) else { throw POSIXError(.EROFS) }
        _ = try await call(11, inode: try id(item))
        return FSOpenItemResult(grantedCoherency: cacheMode == .none ? .noCache : .readCache)
    }
    func close(_ item: FSItem, context: FSContext) async {}
    func upgrade(_ item: FSItem, cacheMode: FSVolume.DataCacheMode, context: FSContext) async throws -> FSUpgradeItemResult {
        _ = try await call(11, inode: try id(item))
        return FSUpgradeItemResult(grantedCoherency: cacheMode == .none ? .noCache : .readCache)
    }
    func xattr(named name: FSFileName, of item: FSItem, context: FSContext) async throws -> FSGetXattrResult {
        guard name.string == "user.sha256" else { throw POSIXError(.ENOATTR) }
        return try checked(FSGetXattrResult(xattrValue: try await call(8, inode: id(item)).payload))
    }
    func xattrs(of item: FSItem, context: FSContext) async throws -> FSListXattrsResult {
        do { _ = try await call(8, inode: id(item)); return try checked(FSListXattrsResult(xattrNames: [FSFileName(string: "user.sha256")])) }
        catch let error as POSIXError where error.code == .ENOATTR { return try checked(FSListXattrsResult(xattrNames: [])) }
    }
    func write(contents: Data, to item: FSItem, at offset: off_t) async throws -> FSWriteFileResult { throw POSIXError(.EROFS) }
    func setAttributes(_ request: FSItem.SetAttributesRequest, on item: FSItem, context: FSContext) async throws -> FSSetAttributesResult { throw POSIXError(.EROFS) }
    func setXattr(named name: FSFileName, to data: Data?, on item: FSItem, policy: FSVolume.SetXattrPolicy, context: FSContext, replyHandler: @escaping @Sendable (FSSetXattrResult?, Error?) -> Void) { replyHandler(nil, POSIXError(.EROFS)) }
    func createItem(named p0: FSFileName, type p1: FSItem.ItemType, in p2: FSItem, attributes p3: FSItem.SetAttributesRequest, context p4: FSContext) async throws -> FSCreateItemResult { throw POSIXError(.EROFS) }
    func createSymbolicLink(named p0: FSFileName, in p1: FSItem, attributes p2: FSItem.SetAttributesRequest, linkContents p3: FSFileName, context p4: FSContext) async throws -> FSCreateSymlinkResult { throw POSIXError(.EROFS) }
    func createLink(to p0: FSItem, named p1: FSFileName, in p2: FSItem, context p3: FSContext) async throws -> FSCreateLinkResult { throw POSIXError(.EROFS) }
    func renameItem(_ p0: FSItem, inDirectory p1: FSItem, named p2: FSFileName, to p3: FSFileName, inDirectory p4: FSItem, overItem p5: FSItem?, context p6: FSContext) async throws -> FSRenameItemResult { throw POSIXError(.EROFS) }
    func removeItem(_ p0: FSItem, named p1: FSFileName, from p2: FSItem, context p3: FSContext) async throws -> FSRemoveItemResult { throw POSIXError(.EROFS) }
}
