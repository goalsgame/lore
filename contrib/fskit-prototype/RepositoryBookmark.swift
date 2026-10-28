// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
import Darwin
import Foundation

let args = CommandLine.arguments
guard args.count == 3, args[1].hasPrefix("/"), args[2].hasPrefix("/") else {
    fputs("usage: repository-bookmark /absolute/checkout /absolute/output\n", stderr)
    exit(2)
}
do {
    let url = URL(fileURLWithPath: args[1], isDirectory: true).standardizedFileURL
    let data = try url.bookmarkData(options: [], includingResourceValuesForKeys: nil, relativeTo: nil)
    guard !data.isEmpty, data.count <= 16_384 else { throw POSIXError(.EFBIG) }
    let fd = Darwin.open(args[2], O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0o600)
    guard fd >= 0 else { throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO) }
    defer { Darwin.close(fd) }
    try data.withUnsafeBytes { bytes in
        var offset = 0
        while offset < bytes.count {
            let count = Darwin.write(fd, bytes.baseAddress!.advanced(by: offset), bytes.count - offset)
            guard count > 0 else { throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO) }
            offset += count
        }
    }
    guard fsync(fd) == 0 else { throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO) }
} catch {
    fputs("Unable to create repository bookmark: \(error)\n", stderr)
    exit(1)
}
