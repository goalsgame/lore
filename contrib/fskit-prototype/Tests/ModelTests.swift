// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
import Foundation

@main
struct ModelTests {
    static func main() throws {
        var model = ProbeModel()
        let original = model.entries[2]!
        try model.switchRevision(1)
        precondition(original.bytes.count == model.entries[2]!.bytes.count)
        precondition(original.digest != model.entries[2]!.digest)
        precondition(model.names["gone.txt"] == nil && model.names["appeared.txt"] == 5)
        precondition(model.entries[model.names["kind"]!]!.kind == .directory)
        precondition(String(decoding: model.entries[8]!.bytes, as: UTF8.self) == "appeared.txt")
        precondition(model.entries[3]!.digest != nil)
        try model.write(Data("EDIT".utf8), id: 3, offset: 0)
        precondition(model.entries[3]!.digest == nil)
        try model.switchRevision(0)
        precondition(model.entries[3]!.digest == nil)
        precondition(model.entries[2]!.digest == original.digest)
        try model.truncate(id: 3, size: 20)
        precondition(model.entries[3]!.bytes.suffix(12) == Data(repeating: 0, count: 12))
        do {
            try model.write(Data([1]), id: 3, offset: Int64.max)
            preconditionFailure("Oversized writes must fail")
        } catch let error as POSIXError { precondition(error.code == .EFBIG) }
        do {
            try model.write(Data([1]), id: 2, offset: 0)
            preconditionFailure("Revision fixtures must be immutable")
        } catch let error as POSIXError { precondition(error.code == .EROFS) }
        print("Probe model tests passed (no FSKit runtime exercised)")
    }
}
