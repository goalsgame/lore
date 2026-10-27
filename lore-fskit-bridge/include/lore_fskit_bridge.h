/* SPDX-FileCopyrightText: 2026 Epic Games, Inc.
 * SPDX-License-Identifier: MIT */
#ifndef LORE_FSKIT_BRIDGE_H
#define LORE_FSKIT_BRIDGE_H
#include <stdint.h>
#if UINTPTR_MAX != UINT64_MAX
#error "Lore FSKit ABI1 requires a64-bit target"
#endif
#define LORE_FSKIT_ABI_VERSION 1u
#define LORE_FSKIT_MAX_READ 1048576u
#define LORE_FSKIT_MAX_PAGE 256u
/* Stable bridge statuses, not platform errno numbers. */
enum { LVFS_OK=0, LVFS_PENDING=1, LVFS_NOT_FOUND=2, LVFS_DENIED=3,
 LVFS_NOT_DIRECTORY=4, LVFS_IS_DIRECTORY=5, LVFS_INVALID=6,
 LVFS_BAD_HANDLE=7, LVFS_STALE=8, LVFS_READ_ONLY=9, LVFS_UNSUPPORTED=10,
 LVFS_NO_ATTRIBUTE=11, LVFS_RANGE=12, LVFS_IO=13, LVFS_CLOSED=14,
 LVFS_LIMIT=15, LVFS_CANCELLED=16, LVFS_PANIC=17, LVFS_BUSY=18,
 LVFS_ABI_MISMATCH=19 };
enum { LVFS_GETATTR=1, LVFS_LOOKUP=2, LVFS_OPEN=3, LVFS_OPENDIR=4,
 LVFS_READ=5, LVFS_READDIR=6, LVFS_RELEASE=7, LVFS_DIGEST=8, LVFS_READLINK=9, LVFS_NAMED_LOOKUP=10, LVFS_REVALIDATE=11 };
typedef struct { uint64_t inode,parent,size; uint32_t mode,kind; } LvfsEntry;
typedef struct { uint32_t opcode,reserved; uint64_t inode,handle,offset;
 uint32_t count,name_length; const uint8_t *name; } LvfsRequest;
typedef struct { int32_t status; uint32_t payload_length; uint64_t value;
 LvfsEntry entry; uint32_t eof,reserved; } LvfsResult;
typedef void (*LvfsCompletion)(void *context, uint64_t operation);
uint32_t lore_fskit_abi_version(void);
/* Bootstrap is blocking and bounded to 30 seconds; call off the main/async thread.
 * Config path is UTF-8, absolute, <=4096 bytes. The file must be private, regular,
 * user-owned, <=8192 bytes JSON. Credentials use the normal Lore pipeline; no
 * tokens belong in the resource URL. A caller cancelled during bootstrap must
 * close a successfully returned session before discarding it. */
int32_t lore_fskit_session_open(uint32_t abi,const uint8_t *config_path,uint32_t length,uint64_t *session);
/* Close must be called off callbacks and Tokio workers. It cancels and drains all
 * bridge futures/completions, invalidates operation/file handles and releases the core.
 * Callbacks must return promptly. Callback-thread reentry returns LVFS_BUSY. */
int32_t lore_fskit_session_close(uint64_t session);
/* Inputs are copied before return. Exactly one completion follows LVFS_OK, which
 * may run before submit returns. No callback follows a rejected submit. */
int32_t lore_fskit_submit(uint64_t session,const LvfsRequest *request,LvfsCompletion completion,void *context,uint64_t *operation);
int32_t lore_fskit_operation_cancel(uint64_t operation);
/* Copies into caller-owned buffers. Required length is always in result.
 * LVFS_RANGE retains the result for retry; NULL payload with capacity0 probes.
 * Readdir payload: little-endian inode,parent,size (u64), mode,kind(u32),
 * cookie(u64), name_length(u32), then raw name bytes, repeated to payload EOF.
 * Named lookup payload is the stored filename (directory scan capped at16384).
 * kind: 1=file,2=directory,3=symlink. No borrowed Rust pointers escape. */
int32_t lore_fskit_operation_result(uint64_t operation,LvfsResult *result,uint8_t *payload,uint32_t capacity);
/* A successful open/opendir owns a file handle only after claim. Releasing an
 * unclaimed result automatically releases it, including cancelled open races. */
int32_t lore_fskit_operation_claim(uint64_t operation);
int32_t lore_fskit_operation_release(uint64_t operation);
int32_t lore_fskit_handle_release(uint64_t session,uint64_t handle);
#ifdef LORE_FSKIT_TEST_FIXTURE
/* Test-only shared-Core fixture; never linked into the extension build. */
int32_t lore_fskit_fixture_open(uint32_t abi,uint64_t *session);
#endif
#endif
