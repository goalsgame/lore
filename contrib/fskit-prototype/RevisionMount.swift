// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
import Darwin
import Foundation

private enum MountError: Error { case invalid(String), system(String) }

private func existingDirectory(_ path: String, allowSymlink: Bool) throws -> URL {
    let raw = URL(fileURLWithPath: path, isDirectory: true).standardizedFileURL
    var rawInfo = stat()
    guard lstat(raw.path, &rawInfo) == 0,
          allowSymlink || (rawInfo.st_mode & S_IFMT) != S_IFLNK else { throw MountError.invalid("Directory is missing or a symlink") }
    let resolved = raw.resolvingSymlinksInPath()
    var info = stat()
    guard stat(resolved.path, &info) == 0, info.st_mode & S_IFMT == S_IFDIR else { throw MountError.invalid("Expected an existing directory") }
    return resolved
}
private func overlaps(_ first: URL, _ second: URL) -> Bool {
    let a = first.pathComponents, b = second.pathComponents
    return a.starts(with: b) || b.starts(with: a)
}
private func privateFile(_ path: URL, data: Data) throws {
    let fd = Darwin.open(path.path, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0o600)
    guard fd >= 0 else { throw MountError.system("Cannot create private bootstrap file") }
    defer { Darwin.close(fd) }
    try data.withUnsafeBytes { bytes in
        var offset = 0
        while offset < bytes.count {
            let n = Darwin.write(fd, bytes.baseAddress!.advanced(by: offset), bytes.count - offset)
            guard n > 0 else { throw MountError.system("Cannot write private bootstrap file") }
            offset += n
        }
    }
    guard fsync(fd) == 0 else { throw MountError.system("Cannot synchronize private bootstrap file") }
}
private func validateTarget(_ target: URL, expected: (dev_t, ino_t)) throws {
    var info = stat()
    guard lstat(target.path, &info) == 0, info.st_mode & S_IFMT == S_IFDIR,
          (info.st_dev, info.st_ino) == expected, info.st_uid == getuid(),
          info.st_mode & 0o022 == 0,
          try FileManager.default.contentsOfDirectory(atPath: target.path).isEmpty else {
        throw MountError.invalid("Mount target changed, is nonempty, or has unsafe ownership")
    }
    var fs = statfs()
    guard statfs(target.path, &fs) == 0 else { throw MountError.system("Cannot inspect mount target") }
    let mounted = withUnsafePointer(to: &fs.f_mntonname) {
        $0.withMemoryRebound(to: CChar.self, capacity: Int(MNAMELEN)) { String(cString: $0) }
    }
    guard URL(fileURLWithPath: mounted).standardizedFileURL.path != target.path else {
        throw MountError.invalid("Target already contains a mount")
    }
}

@main
struct RevisionMount {
    static func main() {
        do {
            let args = CommandLine.arguments
            guard args.count == 5, !args[2].trimmingCharacters(in: .whitespacesAndNewlines).isEmpty,
                  args[2].utf8.count <= 256 else { throw MountError.invalid("Expected repository, revision, target and private config directory") }
            let source = try existingDirectory(args[1], allowSymlink: true)
            let target = try existingDirectory(args[3], allowSymlink: false)
            let configDir = try existingDirectory(args[4], allowSymlink: false)
            var info = stat()
            guard lstat(configDir.path, &info) == 0, info.st_uid == getuid(), info.st_mode & 0o022 == 0,
                  !overlaps(source, target), !overlaps(configDir, target), !overlaps(source, configDir),
                  !target.pathComponents.contains(where: { [".lore", ".urc"].contains($0.lowercased()) }) else {
                throw MountError.invalid("Unsafe or overlapping repository, target, or config directory")
            }
            var targetInfo = stat()
            guard lstat(target.path, &targetInfo) == 0 else { throw MountError.system("Cannot inspect target") }
            let identity = (targetInfo.st_dev, targetInfo.st_ino)
            try validateTarget(target, expected: identity)
            let config = configDir.appendingPathComponent("bootstrap-\(UUID().uuidString).json")
            let object: [String: Any] = ["repository": source.path, "revision": args[2], "offline": true]
            let data = try JSONSerialization.data(withJSONObject: object, options: .sortedKeys)
            guard config.path.utf8.count <= 4096, data.count <= 8192 else { throw MountError.invalid("Bootstrap path or config too long") }
            let bookmark = try source.bookmarkData(options: [], includingResourceValuesForKeys: nil, relativeTo: nil)
            guard !bookmark.isEmpty, bookmark.count <= 16_384 else { throw MountError.invalid("Repository grant too large") }
            try privateFile(config, data: data)
            try privateFile(URL(fileURLWithPath: config.path + ".bookmark"), data: bookmark)
            try validateTarget(target, expected: identity)
            var components = URLComponents()
            components.scheme = "lorerevision"; components.host = "mount"
            components.queryItems = [URLQueryItem(name: "config", value: config.path)]
            guard let resource = components.url?.absoluteString else { throw MountError.invalid("Invalid mount resource") }
            let task = Process()
            task.executableURL = URL(fileURLWithPath: "/sbin/mount")
            task.arguments = ["-r", "-F", "-t", "loreprobe", resource, target.path]
            try task.run(); task.waitUntilExit()
            guard task.terminationReason == .exit, task.terminationStatus == 0 else { throw MountError.system("FSKit mount failed") }
            print("Mounted pinned revision read-only at \(target.path)")
            print("Bootstrap files: \(config.path) and \(config.path).bookmark")
        } catch {
            fputs("FSKit mount failed: \(error)\n", stderr)
            exit(1)
        }
    }
}
