// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
import FSKit
import Foundation
import OSLog

#if !PROBE_VOLUME_TEST
@main
struct ProbeExtension: UnaryFileSystemExtension {
    var fileSystem = ProbeFileSystem()
}
#endif

final class ProbeFileSystem: FSUnaryFileSystem, FSUnaryFileSystemOperations {
    private let revisionLock = NSLock()
    private var revisionVolume: RevisionVolume?
    private var loadedPath: String?
    private var loadingPath: String?
    private var loadGeneration: UInt64 = 0
    private var persistentLoads: [String: PersistentMarkerLoad] = [:]
    private func configPath(_ url: URL) throws -> String {
        guard url.scheme == "lorerevision", url.host == "mount", url.user == nil,
              url.password == nil, url.fragment == nil,
              let components = URLComponents(url: url, resolvingAgainstBaseURL: false),
              let query = components.queryItems, query.count == 1,
              query[0].name == "config", let path = query[0].value,
              path.hasPrefix("/"), path.utf8.count <= 4096 else { throw POSIXError(.EINVAL) }
        return path
    }
    func probeResource(resource: FSResource) async throws -> FSProbeResult {
        guard let resource = resource as? FSGenericURLResource else { return FSProbeResult.notRecognized }
        if resource.url.scheme == "loreprobe" {
            return FSProbeResult.usable(name: "Lore FSKit Probe", containerID: FSContainerIdentifier(uuid: UUID()))
        }
        guard (try? configPath(resource.url)) != nil else { return FSProbeResult.notRecognized }
        return FSProbeResult.usable(name: "Lore pinned revision", containerID: FSContainerIdentifier(uuid: UUID()))
    }
    func loadResource(resource: FSResource, options: FSTaskOptions) async throws -> FSVolume {
        guard let resource = resource as? FSGenericURLResource else { throw POSIXError(.EINVAL) }
        if resource.url.scheme == "loreprobe" {
            let query = URLComponents(url: resource.url, resolvingAgainstBaseURL: false)?.queryItems ?? []
            guard Set(query.map(\.name)).count == query.count,
                  query.allSatisfy({ ["cache", "sync-error", "ids", "revision", "persistent", "create"].contains($0.name) }) else { throw POSIXError(.EINVAL) }
            let cachePolicy = query.first(where: { $0.name == "cache" })?.value ?? "read"
            let syncError = query.first(where: { $0.name == "sync-error" })?.value ?? "0"
            let identifiers = query.first(where: { $0.name == "ids" })?.value ?? "legacy"
            let revision = query.first(where: { $0.name == "revision" })?.value ?? "0"
            guard ["read", "none", "through", "back"].contains(cachePolicy), ["0", "1"].contains(syncError),
                  ["legacy", "reserved"].contains(identifiers), ["0", "1"].contains(revision) else { throw POSIXError(.EINVAL) }
            let runValue = query.first(where: { $0.name == "persistent" })?.value
            let create = query.first(where: { $0.name == "create" })?.value
            guard !query.contains(where: { $0.name == "persistent" }) || runValue != nil,
                  !query.contains(where: { $0.name == "create" }) || create != nil,
                  create == nil || ["0", "1"].contains(create!),
                  runValue != nil || create == nil else { throw POSIXError(.EINVAL) }
            var persistent: (PersistentMarkerWorker, PersistentMarker.Image)?
            if let runValue {
                guard cachePolicy == "none", revision == "0", syncError == "0",
                      let run = UUID(uuidString: runValue), let create else { throw POSIXError(.EINVAL) }
                let parent = URL(fileURLWithPath: NSHomeDirectory(), isDirectory: true)
                    .appendingPathComponent("lore-fskit-persistent-probe", isDirectory: true)
                let key = resource.url.absoluteString
                let load = revisionLock.withLock { () -> PersistentMarkerLoad? in
                    guard persistentLoads[key] == nil else { return nil }
                    let load = PersistentMarkerLoad(parent: parent, run: run, create: create == "1")
                    persistentLoads[key] = load
                    return load
                }
                guard let load else { throw POSIXError(.EBUSY) }
                do {
                    persistent = try await load.value()
                    let accepted = revisionLock.withLock { persistentLoads[key] === load }
                    guard accepted, !Task.isCancelled else { throw POSIXError(.ECANCELED) }
                } catch {
                    revisionLock.withLock { if persistentLoads[key] === load { persistentLoads[key] = nil } }
                    await load.close()
                    throw error
                }
            }
            let volume = ProbeVolume(cachePolicy: cachePolicy, syncError: syncError == "1",
                                     mappedIdentifiers: identifiers == "reserved", revision: revision == "1" ? 1 : 0,
                                     persistent: persistent)
            containerStatus = .ready
            return volume
        }
        let path = try configPath(resource.url)
        let generation = try revisionLock.withLock { () throws -> UInt64 in
            guard revisionVolume == nil, loadingPath == nil else { throw POSIXError(.EBUSY) }
            loadGeneration &+= 1; loadingPath = path; return loadGeneration
        }
        defer { revisionLock.withLock { if loadGeneration == generation { loadingPath = nil } } }
        let session: RustSession
        do {
            let access = try RepositoryScope.open(configPath: path)
            session = try await RustSession.open(configPath: path, access: access)
        }
        catch let error as RustBridgeError {
            throw NSError(domain: "LoreFSKitBootstrap", code: Int(error.status), userInfo: [
                NSLocalizedDescriptionKey: "Shared Lore revision setup failed (status \(error.status)). The extension must have access to the configured repository, normal credentials and content stores."
            ])
        }
        let volume = RevisionVolume(session: session)
        let accepted = revisionLock.withLock { () -> Bool in
            guard revisionVolume == nil, loadGeneration == generation, loadingPath == path,
                  !Task.isCancelled else { return false }
            revisionVolume = volume; loadedPath = path; loadingPath = nil; return true
        }
        guard accepted else { await session.close(); throw POSIXError(.ECANCELED) }
        containerStatus = .ready
        return volume
    }
    func unloadResource(resource: FSResource, options: FSTaskOptions) async throws {
        defer { containerStatus = .notReady(status: POSIXError(.ENXIO) as NSError) }
        guard let resource = resource as? FSGenericURLResource else { return }
        if resource.url.scheme == "loreprobe" {
            let load = revisionLock.withLock { persistentLoads.removeValue(forKey: resource.url.absoluteString) }
            if let load { await load.close() }
            return
        }
        guard let path = try? configPath(resource.url) else { return }
        let volume = revisionLock.withLock { () -> RevisionVolume? in
            guard loadedPath == path || loadingPath == path else { return nil }
            loadGeneration &+= 1; loadedPath = nil; loadingPath = nil
            let result = revisionVolume; revisionVolume = nil; return result
        }
        if let volume { try await volume.shutdown() }
    }
}

final class ProbeItem: FSItem {
    let id: UInt64
    init(_ id: UInt64) { self.id = id; super.init() }
}

final class ProbeVolume: FSVolume, FSVolume.Handler, FSVolume.ReadWriteHandler,
                         FSVolume.XattrHandler, FSVolume.DataCacheHandler, @unchecked Sendable {
    private let lock = NSLock()
    private var model = ProbeModel()
    private let persistent: PersistentMarkerWorker?
    private var persistentSequence: UInt64 = 0
    private let items = Dictionary(uniqueKeysWithValues: (UInt64(1)...8).map { ($0, ProbeItem($0)) })
    private var status = "ready:0"
    private var switching = false
    private var observed: Set<UInt64> = [1]
    private var failure: Error?
    private var syncCalls: UInt64 = 0
    private var syncFlags: [Int] = []
    private var lookupCalls: UInt64 = 0
    private var lookupMisses: UInt64 = 0
    private var events: [String] = []
    private var diagnostic: [String: Any] = [:]
    private let cachePolicy: String
    private let syncError: Bool
    private let mappedIdentifiers: Bool
    private let diagnosticID = UUID().uuidString
    private let logger = Logger(subsystem: "org.example.lore.fskit-probe", category: "diagnostics")
    private let transitions = DispatchQueue(label: "org.example.lore.fskit-probe.transitions")
    static let switchAttribute = "user.lore.prototype.switch"
    static let statusAttribute = "user.lore.prototype.status"
    static let statsAttribute = "user.lore.prototype.stats"
    static let cacheAttribute = "user.lore.prototype.cache"

    init(cachePolicy: String = "read", syncError: Bool = false, mappedIdentifiers: Bool = false, revision: Int = 0,
         persistent: (PersistentMarkerWorker, PersistentMarker.Image)? = nil) {
        self.cachePolicy = cachePolicy
        self.syncError = syncError
        self.mappedIdentifiers = mappedIdentifiers
        self.persistent = persistent?.0
        super.init(volumeID: FSVolume.Identifier(uuid: UUID()),
                   volumeName: FSFileName(string: "Lore FSKit Probe"))
        if revision == 1 { try! model.switchRevision(1); status = "ready:1" }
        if let image = persistent?.1 {
            persistentSequence = image.sequence
            model.entries[3]!.bytes = image.bytes
            model.entries[3]!.clean = false
        }

    }

    var maximumLinkCount: Int { 1 }
    var maximumNameLength: Int { 255 }
    var maximumXattrSize: Int { 4096 }
    var maximumFileSize: UInt64 { UInt64(ProbeModel.capacity) }
    var restrictsOwnershipChanges: Bool { true }
    var truncatesLongNames: Bool { false }
    var isDataCacheInhibited: Bool { false }
    var supportedVolumeCapabilities: FSVolume.SupportedCapabilities {
        let caps = FSVolume.SupportedCapabilities()
        caps.supportsSymbolicLinks = true
        caps.caseFormat = .sensitive
        return caps
    }
    var volumeStatistics: FSStatFSResult {
        let stats = FSStatFSResult(fileSystemTypeName: "loreprobe")
        stats.blockSize = 4096
        stats.ioSize = 65536
        stats.totalBytes = UInt64(ProbeModel.capacity)
        stats.freeBytes = UInt64(ProbeModel.capacity / 2)
        stats.availableBytes = stats.freeBytes
        stats.totalFiles = 8
        return stats
    }

    private func id(_ item: FSItem) throws -> UInt64 {
        guard let item = item as? ProbeItem else { throw POSIXError(.ESTALE) }
        return item.id
    }

    private func snapshot(_ item: FSItem) throws -> (ProbeModel.Entry, FSItem.Attributes) {
        let key = try id(item)
        return try lock.withLock {
            if let failure { throw failure }
            guard let entry = model.entries[key] else { throw POSIXError(.ESTALE) }
            return (entry, attributes(entry))
        }
    }

    private func kernelID(_ key: UInt64) -> FSItem.Identifier {
        if !mappedIdentifiers { return FSItem.Identifier(key) }
        return key == 1 ? .rootDirectory : FSItem.Identifier(key + 2)
    }

    // Call while holding lock so FSKit attribute sequence numbers follow model generations.
    private func attributes(_ entry: ProbeModel.Entry) -> FSItem.Attributes {
        let attr = FSItem.Attributes()
        attr.fileID = kernelID(entry.id)
        attr.parentID = mappedIdentifiers ? (entry.id == 1 ? .parentOfRoot : .rootDirectory) : FSItem.Identifier(1)
        attr.type = entry.kind == .directory ? .directory : entry.kind == .symlink ? .symlink : .file
        attr.mode = entry.kind == .directory ? 0o755 : entry.id == 3 ? 0o644 : 0o444
        attr.uid = getuid()
        attr.gid = getgid()
        attr.linkCount = 1
        attr.size = UInt64(entry.bytes.count)
        attr.allocSize = UInt64(entry.bytes.count)
        attr.flags = 0
        attr.inhibitKernelOffloadedIO = true
        let time = timespec(tv_sec: 1_700_000_000 + Int(entry.generation), tv_nsec: 0)
        attr.modifyTime = time
        attr.changeTime = time
        attr.birthTime = time
        attr.accessTime = time
        attr.addedTime = time
        attr.backupTime = timespec(tv_sec: 0, tv_nsec: 0)
        return attr
    }

    private func checked<T>(_ result: T?) throws -> T {
        guard let result else { throw POSIXError(.EIO) }
        return result
    }

    func activateVolume(options: FSTaskOptions) async throws -> FSActivateResult {
        try checked(FSActivateResult(rootItem: items[1]!))
    }
    func deactivateVolume(options: FSDeactivateOptions) async throws {}
    func mount(options: FSTaskOptions) async throws {}
    func unmount() async { await persistent?.closeAndDrain() }
    func synchronize(flags: FSSyncFlags) async throws {
        lock.withLock {
            syncCalls += 1; syncFlags.append(flags.rawValue)
            if syncFlags.count > 12 { syncFlags.removeFirst() }
        }
        logger.notice("synchronize volume=\(self.diagnosticID, privacy: .public) flags=\(flags.rawValue) injectedError=\(self.syncError)")
        if syncError { throw POSIXError(.EIO) }
    }
    private func record(_ event: String) {
        lock.withLock {
            events.append(event)
            if events.count > 12 { events.removeFirst() }
        }
    }
    func reclaimItem(_ item: FSItem) async throws {
        let key = try id(item)
        record("reclaim:\(key)")
        _ = lock.withLock { observed.remove(key) }
    }

    func lookupItem(named name: FSFileName, in directory: FSItem, context: FSContext) async throws -> FSLookupItemResult {
        let (parent, _) = try snapshot(directory)
        guard parent.kind == .directory else { throw POSIXError(.ENOTDIR) }
        let (found, attr) = try lock.withLock {
            if let failure { throw failure }
            lookupCalls += 1
            guard parent.id == 1, let key = model.names[name.string ?? ""],
                  let found = model.entries[key] else { lookupMisses += 1; throw POSIXError(.ENOENT) }
            observed.insert(key)
            return (found, attributes(found))
        }
        return try checked(FSLookupItemResult(foundItem: items[found.id]!, itemName: name,
                                             itemAttributes: attr))
    }

    func attributes(_ request: FSItem.GetAttributesRequest, of item: FSItem, context: FSContext) async throws -> FSGetAttributesResult {
        try checked(FSGetAttributesResult(attributes: try snapshot(item).1))
    }

    func enumerateDirectory(_ directory: FSItem, startingAt cookie: FSDirectoryCookie,
                            verifier: FSDirectoryVerifier, attributes request: FSItem.GetAttributesRequest?,
                            packer: FSDirectoryEntryPacker, context: FSContext) async throws -> FSEnumerateDirectoryResult {
        let (parent, _) = try snapshot(directory)
        guard parent.kind == .directory else { throw POSIXError(.ENOTDIR) }
        return try lock.withLock {
            if let failure { throw failure }
            let currentVerifier = model.entries[1]!.generation
            guard cookie.rawValue == 0 || verifier.rawValue == currentVerifier else { throw POSIXError(.ESTALE) }
            let names = parent.id == 1 ? model.names.keys.sorted() : []
            for index in Int(min(cookie.rawValue, UInt64(names.count)))..<names.count {
                let name = names[index]
                let child = model.entries[model.names[name]!]!
                let attr = attributes(child)
                if !packer.packEntry(name: FSFileName(string: name), itemType: attr.type,
                                     itemID: attr.fileID, nextCookie: FSDirectoryCookie(UInt64(index + 1)),
                                     attributes: request == nil ? nil : attr) { break }
                observed.insert(child.id)
            }
            return try checked(FSEnumerateDirectoryResult(verifier: currentVerifier))
        }
    }

    func readSymbolicLink(_ item: FSItem, context: FSContext) async throws -> FSReadSymlinkResult {
        let (value, attr) = try snapshot(item)
        guard value.kind == .symlink else { throw POSIXError(.EINVAL) }
        return try checked(FSReadSymlinkResult(contents: FSFileName(string: String(decoding: value.bytes, as: UTF8.self)), symlinkAttributes: attr))
    }

    func read(from item: FSItem, at offset: off_t, length: Int, into buffer: FSMutableFileDataBuffer) async throws -> FSReadFileResult {
        let (value, attr) = try snapshot(item)
        guard value.kind == .file, offset >= 0, length >= 0 else { throw POSIXError(.EINVAL) }
        let start = Int(min(offset, Int64(value.bytes.count)))
        let count = min(length, value.bytes.count - start)
        try buffer.withUnsafeMutableBytes { output in
            guard count <= output.count else { throw POSIXError(.EIO) }
            value.bytes.withUnsafeBytes { input in
                if count > 0 { output.baseAddress!.copyMemory(from: input.baseAddress! + start, byteCount: count) }
            }
        }
        lock.withLock { model.reads += 1 }
        return try checked(FSReadFileResult(bytesRead: count, itemAttributes: attr))
    }

    func write(contents: Data, to item: FSItem, at offset: off_t) async throws -> FSWriteFileResult {
        let key = try id(item)
        if let persistent {
            guard key == 3 else { throw POSIXError(.EROFS) }
            try lock.withLock { if let failure { throw failure } }
            guard offset >= 0, offset <= ProbeModel.capacity,
                  contents.count <= ProbeModel.capacity - Int(offset) else { throw POSIXError(.EFBIG) }
            let image: PersistentMarker.Image
            do { image = try await persistent.write(contents, at: offset) }
            catch { lock.withLock { failure = POSIXError(.EIO) }; throw POSIXError(.EIO) }
            let attr = lock.withLock { applyPersistent(image) }
            return try checked(FSWriteFileResult(bytesWritten: contents.count, itemAttributes: attr, freeSpace: nil))
        }
        let attr = try lock.withLock {
            if let failure { throw failure }
            try model.write(contents, id: key, offset: offset)
            return attributes(model.entries[key]!)
        }
        return try checked(FSWriteFileResult(bytesWritten: contents.count, itemAttributes: attr, freeSpace: nil))
    }

    func setAttributes(_ request: FSItem.SetAttributesRequest, on item: FSItem, context: FSContext) async throws -> FSSetAttributesResult {
        let key = try id(item)
        guard request.isValid(.size) else { throw POSIXError(.ENOTSUP) }
        if let persistent {
            guard key == 3 else { throw POSIXError(.EROFS) }
            try lock.withLock { if let failure { throw failure } }
            guard request.size <= ProbeModel.capacity else { throw POSIXError(.EFBIG) }
            let image: PersistentMarker.Image
            do { image = try await persistent.truncate(request.size) }
            catch { lock.withLock { failure = POSIXError(.EIO) }; throw POSIXError(.EIO) }
            let attr = lock.withLock { applyPersistent(image) }
            request.consumedAttributes = .size
            return try checked(FSSetAttributesResult(attributes: attr, freeSpace: nil))
        }
        let attr = try lock.withLock {
            if let failure { throw failure }
            try model.truncate(id: key, size: request.size)
            return attributes(model.entries[key]!)
        }
        request.consumedAttributes = .size
        return try checked(FSSetAttributesResult(attributes: attr, freeSpace: nil))
    }

    // Called with lock held; concurrent completions must never install an older image.
    private func applyPersistent(_ image: PersistentMarker.Image) -> FSItem.Attributes {
        if image.sequence > persistentSequence {
            persistentSequence = image.sequence
            model.entries[3]!.bytes = image.bytes
            model.entries[3]!.clean = false
            model.entries[3]!.generation += 1
        }
        return attributes(model.entries[3]!)
    }

    private func coherency(_ mode: FSVolume.DataCacheMode) -> FSVolume.KernelCacheCoherencyType {
        if mode == .none || cachePolicy == "none" { return .noCache }
        if mode == .readWriteWithCache {
            if cachePolicy == "through" { return .writeThrough }
            if cachePolicy == "back" { return .writeBack }
        }
        return .readCache
    }
    func open(_ item: FSItem, modes: FSVolume.OpenModes, cacheMode: FSVolume.DataCacheMode, context: FSContext) async throws -> FSOpenItemResult {
        let (value, _) = try snapshot(item)
        if modes.contains(.write) {
            guard value.id == 3 else { throw POSIXError(.EROFS) }
            // A writable mapping may dirty kernel pages before a write callback arrives.
            lock.withLock { model.entries[3]!.clean = false }
        }
        let granted = coherency(cacheMode)
        record("open:\(value.id):\(cacheMode.rawValue):\(granted.rawValue)")
        return FSOpenItemResult(grantedCoherency: granted)
    }
    func close(_ item: FSItem, context: FSContext) async { if let key = try? id(item) { record("close:\(key)") } }
    func upgrade(_ item: FSItem, cacheMode: FSVolume.DataCacheMode, context: FSContext) async throws -> FSUpgradeItemResult {
        let (value, _) = try snapshot(item)
        if cacheMode == .readWriteWithCache && value.id == 3 {
            lock.withLock { model.entries[3]!.clean = false }
        }
        let granted = coherency(cacheMode)
        record("upgrade:\(value.id):\(cacheMode.rawValue):\(granted.rawValue)")
        return FSUpgradeItemResult(grantedCoherency: granted)
    }

    func xattr(named name: FSFileName, of item: FSItem, context: FSContext) async throws -> FSGetXattrResult {
        let key = try id(item)
        let value: Data = try lock.withLock {
            if key == 1 {
                switch name.string {
                case Self.statusAttribute: return Data(status.utf8)
                case Self.statsAttribute:
                    return try JSONSerialization.data(withJSONObject: [
                        "probe_version": 2, "probe_id": diagnosticID, "ids": mappedIdentifiers ? "reserved" : "legacy",
                        "reads": model.reads, "digest_requests": model.digestRequests,
                        "persistent_sequence": persistentSequence,
                        "sync_calls": syncCalls, "sync_flags": syncFlags, "sync_error": syncError,
                        "lookups": lookupCalls, "lookup_misses": lookupMisses, "events": events,
                        "cache_policy": cachePolicy, "diagnostic": diagnostic,
                    ], options: [.sortedKeys])
                default: throw POSIXError(.ENOATTR)
                }
            }
            if let failure { throw failure }
            guard name.string == "user.sha256" else { throw POSIXError(.ENOATTR) }
            model.digestRequests += 1
            guard let digest = model.entries[key]?.digest else { throw POSIXError(.ENOATTR) }
            return digest
        }
        return try checked(FSGetXattrResult(xattrValue: value))
    }

    func xattrs(of item: FSItem, context: FSContext) async throws -> FSListXattrsResult {
        let key = try id(item)
        let names = lock.withLock {
            key == 1 ? [Self.statusAttribute, Self.statsAttribute] : model.entries[key]?.digest == nil ? [] : ["user.sha256"]
        }
        return try checked(FSListXattrsResult(xattrNames: names.map { FSFileName(string: $0) }))
    }

    // Return before cache control: the kernel may reenter the module during invalidation.
    // This control changes the model directly; it does not rename/write fixture paths through VFS.
    func setXattr(named name: FSFileName, to data: Data?, on item: FSItem,
                  policy: FSVolume.SetXattrPolicy, context: FSContext,
                  replyHandler: @escaping @Sendable (FSSetXattrResult?, Error?) -> Void) {
        do {
            if name.string == Self.cacheAttribute {
                guard [UInt64(1), 2].contains(try id(item)), policy == .alwaysSet, let data,
                      data.count <= 512 else { throw POSIXError(.EINVAL) }
                let command = try ProbeCacheCommand(data: data)
                try lock.withLock {
                    guard !switching, observed.contains(command.item), failure == nil else { throw POSIXError(.EBUSY) }
                    diagnostic = ["token": command.token, "status": "pending"]
                }
                replyHandler(FSSetXattrResult(freeSpace: nil), nil)
                transitions.asyncAfter(deadline: .now() + .milliseconds(command.delayMS)) {
                    let error = self.setCacheState(for: self.items[command.item]!, cacheMode: command.mode,
                                                  coherencyType: command.coherency, action: command.action)
                    self.lock.withLock {
                        self.diagnostic = command.result(error)
                    }
                    if let error = error as NSError? {
                        self.logger.notice("cache volume=\(self.diagnosticID, privacy: .public) item=\(command.item) domain=\(error.domain, privacy: .public) code=\(error.code)")
                    }
                }
                return
            }
            guard try id(item) == 1, name.string == Self.switchAttribute else { throw POSIXError(.EPERM) }
            guard policy == .alwaysSet, let data,
                  let next = Int(String(decoding: data, as: UTF8.self)), next == 0 || next == 1 else { throw POSIXError(.EINVAL) }
            try lock.withLock {
                guard !switching else { throw POSIXError(.EBUSY) }
                if let failure { throw failure }
                switching = true
                status = "switching:\(next)"
            }
            replyHandler(FSSetXattrResult(freeSpace: nil), nil)
            transitions.async { self.switchRevision(next) }
        } catch { replyHandler(nil, error) }
    }

    private func switchRevision(_ next: Int) {
        do {
            // Fixture files are immutable; edit.txt is deliberately outside revision switches.
            let cached = try lock.withLock {
                try model.switchRevision(next)
                // Directory and symlink invalidation remain gated on the native
                // namespace test, including negative lookups.
                return observed.filter { $0 != 3 && model.entries[$0]?.kind == .file }.sorted()
            }
            for key in cached {
                if let error = setCacheState(for: items[key]!, cacheMode: .none,
                                            coherencyType: .noCache, action: .invalidate) {
                    throw NSError(domain: "LoreProbe.CacheTransition", code: Int(key), userInfo: [
                        NSLocalizedDescriptionKey: "Cache invalidation failed for fixture item \(key): \(error)",
                        NSUnderlyingErrorKey: error,
                    ])
                }
            }
            lock.withLock { status = "ready:\(next)"; switching = false }
        } catch {
            lock.withLock { failure = error; status = "failed:\(error)"; switching = false }
        }
    }
    func createItem(named p0: FSFileName, type p1: FSItem.ItemType, in p2: FSItem, attributes p3: FSItem.SetAttributesRequest, context p4: FSContext) async throws -> FSCreateItemResult { throw POSIXError(.EROFS) }
    func createSymbolicLink(named p0: FSFileName, in p1: FSItem, attributes p2: FSItem.SetAttributesRequest, linkContents p3: FSFileName, context p4: FSContext) async throws -> FSCreateSymlinkResult { throw POSIXError(.EROFS) }
    func createLink(to p0: FSItem, named p1: FSFileName, in p2: FSItem, context p3: FSContext) async throws -> FSCreateLinkResult { throw POSIXError(.EROFS) }
    func renameItem(_ p0: FSItem, inDirectory p1: FSItem, named p2: FSFileName, to p3: FSFileName, inDirectory p4: FSItem, overItem p5: FSItem?, context p6: FSContext) async throws -> FSRenameItemResult { throw POSIXError(.EROFS) }
    func removeItem(_ p0: FSItem, named p1: FSFileName, from p2: FSItem, context p3: FSContext) async throws -> FSRemoveItemResult { throw POSIXError(.EROFS) }
}
