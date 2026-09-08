// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Encoding of a [`Fragment`] as GCS object custom metadata.
//!
//! Ported from `lore_aws::store::object_metadata`, which documents the design rationale in full
//! (ADR-00018): the fragment describing a payload travels on the GCS object holding that
//! payload, as a custom metadata entry, rather than in a separate Firestore document. GCS object
//! metadata is part of the object generation: `write_object` sets it in the same call that
//! uploads the body, and a `read_object`/`get_object` response returns headers and body (or
//! headers alone) from that same generation. A reader therefore always sees the fragment that was
//! written with the bytes it is reading — no write protocol is involved, only GCS's own object
//! atomicity, exactly as for S3.
//!
//! The wire format is unchanged from the S3 encoding on purpose: `<flags>:<size_payload>:<size_content>`,
//! flags in hex and sizes in decimal, under a single key. Keeping the same format (rather than,
//! say, one custom-metadata entry per field) means this module is a copy of the S3 one with only
//! the doc comments and the key name adjusted for GCS's naming convention.
//!
//! Only the representation is carried here. Obliteration state is mutable and lives in
//! Firestore; see [`crate::store::immutable_store`].

use std::collections::HashMap;

use lore_base::types::Fragment;
use lore_base::types::FragmentFlags;

/// The flags that describe the payload itself, and therefore the only ones the object carries.
/// See `lore_aws::store::object_metadata::PAYLOAD_FLAGS` for the full rationale, which applies
/// unchanged here.
pub const PAYLOAD_FLAGS: u32 = FragmentFlags::PayloadFragmented.bits()
    | FragmentFlags::PayloadCompressed.bits()
    | FragmentFlags::PayloadRevisionState.bits();

/// The single object metadata key holding the whole fragment. GCS custom metadata keys are
/// case-sensitive and returned as given, unlike S3's lowercasing — written lowercase anyway for
/// consistency with the S3 encoding and with `x-goog-meta-*` convention (all lowercase in
/// practice).
const KEY_FRAGMENT: &str = "lore-fragment";

/// Separator between the fragment's fields within [`KEY_FRAGMENT`].
const SEPARATOR: char = ':';

/// Names for the parts of the value, used only to say which one failed to parse.
const FIELD_COUNT: &str = "field count";
const FIELD_FLAGS: &str = "flags";
const FIELD_SIZE_PAYLOAD: &str = "size_payload";
const FIELD_SIZE_CONTENT: &str = "size_content";

/// Why a stored object did not yield a usable fragment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectMetadataError {
    /// The object carries no lore metadata at all.
    Absent,
    /// The object carries lore metadata that could not be parsed. Names the offending field.
    Malformed(&'static str),
}

impl std::error::Error for ObjectMetadataError {}

impl std::fmt::Display for ObjectMetadataError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Absent => write!(f, "object carries no fragment metadata"),
            Self::Malformed(field) => write!(f, "fragment metadata field {field} is malformed"),
        }
    }
}

/// Render a fragment as the custom metadata map to attach to its object, via
/// `WriteObject::set_metadata` in the same call that uploads the body.
pub fn to_object_metadata(fragment: &Fragment) -> HashMap<String, String> {
    HashMap::from([(KEY_FRAGMENT.to_owned(), encode(fragment))])
}

fn encode(fragment: &Fragment) -> String {
    let flags = fragment.flags & PAYLOAD_FLAGS;

    format!(
        "{flags:x}{SEPARATOR}{}{SEPARATOR}{}",
        fragment.size_payload, fragment.size_content
    )
}

/// Recover the fragment from the custom metadata on a `GetObject`/`ReadObject` response.
pub fn from_object_metadata(
    metadata: &HashMap<String, String>,
) -> Result<Fragment, ObjectMetadataError> {
    decode(metadata.get(KEY_FRAGMENT).ok_or(ObjectMetadataError::Absent)?)
}

fn decode(value: &str) -> Result<Fragment, ObjectMetadataError> {
    let mut fields = value.split(SEPARATOR);
    let malformed = || ObjectMetadataError::Malformed(FIELD_COUNT);

    let flags = fields.next().ok_or_else(malformed)?;
    let size_payload = fields.next().ok_or_else(malformed)?;
    let size_content = fields.next().ok_or_else(malformed)?;

    if fields.next().is_some() {
        return Err(malformed());
    }

    Ok(Fragment {
        flags: u32::from_str_radix(flags, 16)
            .map_err(|_parse| ObjectMetadataError::Malformed(FIELD_FLAGS))?
            & PAYLOAD_FLAGS,
        size_payload: size_payload
            .parse()
            .map_err(|_parse| ObjectMetadataError::Malformed(FIELD_SIZE_PAYLOAD))?,
        size_content: size_content
            .parse()
            .map_err(|_parse| ObjectMetadataError::Malformed(FIELD_SIZE_CONTENT))?,
    })
}

#[cfg(test)]
mod test {
    use super::*;

    fn fragment() -> Fragment {
        Fragment {
            flags: FragmentFlags::PayloadCompressedZstd.bits(),
            size_payload: 4096,
            size_content: 16384,
        }
    }

    #[test]
    fn round_trips_a_fragment() {
        let encoded = to_object_metadata(&fragment());

        assert_eq!(from_object_metadata(&encoded), Ok(fragment()));
    }

    #[test]
    fn writes_one_key_holding_hex_flags_and_decimal_sizes() {
        let encoded = to_object_metadata(&fragment());

        assert_eq!(encoded.len(), 1, "one key, not one per field");
        assert_eq!(encoded.get(KEY_FRAGMENT).unwrap(), "8:4096:16384");
    }

    #[test]
    fn round_trips_the_extreme_values() {
        let extreme = Fragment {
            flags: PAYLOAD_FLAGS,
            size_payload: u32::MAX,
            size_content: u64::MAX,
        };
        let encoded = to_object_metadata(&extreme);

        assert_eq!(from_object_metadata(&encoded), Ok(extreme));
    }

    #[test]
    fn drops_state_store_location_and_per_machine_flags() {
        for flag in [
            FragmentFlags::PayloadObliterating,
            FragmentFlags::PayloadObliterated,
            FragmentFlags::PayloadStoredDurable,
            FragmentFlags::PayloadStoredLocal,
            FragmentFlags::PayloadLocalCachePriority,
            FragmentFlags::PayloadDoNotReplicate,
        ] {
            let mut tainted = fragment();
            tainted.flags |= flag.bits();

            assert_eq!(
                from_object_metadata(&to_object_metadata(&tainted)),
                Ok(fragment()),
                "{flag:?} is not a property of the payload and must not travel on the object"
            );
        }
    }

    #[test]
    fn reports_an_object_with_no_metadata_as_absent() {
        assert_eq!(
            from_object_metadata(&HashMap::new()),
            Err(ObjectMetadataError::Absent)
        );
        assert_eq!(
            from_object_metadata(&HashMap::from([(
                "unrelated".to_owned(),
                "8:1:1".to_owned()
            )])),
            Err(ObjectMetadataError::Absent)
        );
    }

    #[test]
    fn reports_the_wrong_number_of_fields_as_malformed() {
        for value in ["", "8", "8:1", "8:1:1:1"] {
            let encoded = HashMap::from([(KEY_FRAGMENT.to_owned(), value.to_owned())]);

            assert_eq!(
                from_object_metadata(&encoded),
                Err(ObjectMetadataError::Malformed(FIELD_COUNT)),
                "{value:?} does not hold three fields"
            );
        }
    }

    #[test]
    fn names_the_field_that_failed_to_parse() {
        for (value, field) in [
            ("zz:1:1", FIELD_FLAGS),
            ("8:nope:1", FIELD_SIZE_PAYLOAD),
            ("8:1:nope", FIELD_SIZE_CONTENT),
        ] {
            let encoded = HashMap::from([(KEY_FRAGMENT.to_owned(), value.to_owned())]);

            assert_eq!(
                from_object_metadata(&encoded),
                Err(ObjectMetadataError::Malformed(field))
            );
        }
    }

    #[test]
    fn reads_flags_as_hex() {
        let encoded = HashMap::from([(KEY_FRAGMENT.to_owned(), "a:1:1".to_owned())]);

        assert_eq!(
            from_object_metadata(&encoded).unwrap().flags,
            0xa & PAYLOAD_FLAGS
        );
    }
}
