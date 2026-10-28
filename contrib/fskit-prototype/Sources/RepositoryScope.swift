// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
import Darwin
import Foundation

/// Retains a single checkout grant until all bootstrap or session I/O has ended.
final class RepositoryScope: @unchecked Sendable {
    private static let retainedLock = NSLock()
    private nonisolated(unsafe) static var uncertain: [RepositoryScope] = []
    private let url: URL
    private init(_ url: URL) { self.url = url }
    deinit { url.stopAccessingSecurityScopedResource() }

    static func retainIfUncertain(_ access: RepositoryScope?, status: Int32) {
        guard status != 0, status != 7, let access else { return }
        retainedLock.withLock { uncertain.append(access) }
    }

    private static func privateData(_ path: String, maximum: Int) throws -> Data? {
        let fd = Darwin.open(path, O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC)
        if fd < 0 {
            if errno == ENOENT { return nil }
            throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
        }
        let handle = FileHandle(fileDescriptor: fd, closeOnDealloc: true)
        var info = stat()
        guard fstat(fd, &info) == 0, info.st_mode & S_IFMT == S_IFREG,
              info.st_uid == getuid(), info.st_mode & 0o077 == 0,
              info.st_nlink == 1, info.st_size >= 0, info.st_size <= maximum else { throw POSIXError(.EINVAL) }
        let data = try handle.read(upToCount: maximum + 1) ?? Data()
        guard data.count <= maximum else { throw POSIXError(.EFBIG) }
        return data
    }

    static func open(configPath: String) throws -> RepositoryScope? {
        guard let bookmark = try privateData(configPath + ".bookmark", maximum: 16_384) else { return nil }
        guard retainedLock.withLock({ uncertain.count < 32 }) else { throw POSIXError(.EBUSY) }
        guard let config = try privateData(configPath, maximum: 8192),
              let fields = try JSONSerialization.jsonObject(with: config) as? [String: Any],
              let repository = fields["repository"] as? String, repository.hasPrefix("/") else {
            throw POSIXError(.EINVAL)
        }
        var stale = false
        let url = try URL(resolvingBookmarkData: bookmark, options: [.withoutUI, .withoutImplicitStartAccessing],
                          relativeTo: nil, bookmarkDataIsStale: &stale)
        guard !stale, url.isFileURL,
              url.standardizedFileURL.path == URL(fileURLWithPath: repository).standardizedFileURL.path,
              url.startAccessingSecurityScopedResource() else { throw POSIXError(.EACCES) }
        return RepositoryScope(url)
    }
}
