// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
import SwiftUI

@main
struct ProbeHost: App {
    var body: some Scene {
        WindowGroup("Lore FSKit Prototype") {
            VStack(alignment: .leading, spacing: 16) {
                Text("Lore FSKit Prototype").font(.title)
                Text("Enable the Lore filesystem extension in System Settings → General → Login Items & Extensions → File System Extensions.")
                Text("Use the repository's mount-test instructions to mount the disposable fixtures. This app does not mount or modify a Lore checkout.")
            }.padding(24).frame(width: 520)
        }
    }
}
