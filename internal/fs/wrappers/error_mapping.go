// Copyright 2021 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package wrappers

import (
	"context"
	"errors"
	"log"
	"net/http"
	"strings"
	"syscall"

	"cloud.google.com/go/storage"
	"github.com/googlecloudplatform/gcsfuse/internal/logger"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
	"google.golang.org/api/googleapi"
)

var (
	DefaultFSError = syscall.EIO
)

func errno(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}

	// Context will be cancelled only when kernel sends an interrupt.
	// Jacobsa/gcloud is returning net/http: request cancelled instead of context
	// cancelled. So checking explicitly if the context is cancelled to return
	// correct error to kernel.
	// This issue doesn't exist when go-storage client library is used. It returns
	// context cancelled error with sub message as request cancelled.
	//
	// TODO: Remove this logic when go-storage client library integration is
	// production ready.
	contextErr := ctx.Err()
	if errors.Is(contextErr, context.Canceled) {
		return syscall.EINTR
	}

	// Use existing em errno
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno
	}

	// The fuse op is interrupted
	if errors.Is(err, context.Canceled) {
		return syscall.EINTR
	}
	if errors.Is(err, storage.ErrObjectNotExist) {
		return syscall.ENOENT
	}

	// The HTTP request is canceled
	if strings.Contains(err.Error(), "net/http: request canceled") {
		return syscall.ECANCELED
	}

	// Cannot authenticate
	if strings.Contains(err.Error(), "oauth2: cannot fetch token") {
		return syscall.EACCES
	}

	// Translate API errors into an em errno
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) {
		switch apiErr.Code {
		case http.StatusForbidden:
			return syscall.EACCES
		case http.StatusNotFound:
			return syscall.ENOENT
		}
	}

	return DefaultFSError
}

// WithErrorMapping wraps a FileSystem, processing the returned errors, and
// mapping them into syscall.Errno that can be understood by FUSE.
func WithErrorMapping(wrapped fuseutil.FileSystem) fuseutil.FileSystem {
	return &errorMapping{
		wrapped: wrapped,
		logger:  logger.NewError(""),
	}
}

type errorMapping struct {
	wrapped fuseutil.FileSystem
	logger  *log.Logger
}

func (em *errorMapping) mapError(ctx context.Context, op string, err error) error {
	fsErr := errno(ctx, err)
	if err != nil && fsErr != nil && err != fsErr {
		em.logger.Printf("%s: %v, %v", op, fsErr, err)
	}
	return fsErr
}

func (em *errorMapping) Destroy() {
	em.wrapped.Destroy()
}

func (em *errorMapping) StatFS(
	ctx context.Context,
	op *fuseops.StatFSOp) error {
	err := em.wrapped.StatFS(ctx, op)
	return em.mapError(ctx, "StatFS", err)
}

func (em *errorMapping) LookUpInode(
	ctx context.Context,
	op *fuseops.LookUpInodeOp) error {
	err := em.wrapped.LookUpInode(ctx, op)
	return em.mapError(ctx, "LookUpInode", err)
}

func (em *errorMapping) GetInodeAttributes(
	ctx context.Context,
	op *fuseops.GetInodeAttributesOp) error {
	err := em.wrapped.GetInodeAttributes(ctx, op)
	return em.mapError(ctx, "GetInodeAttributes", err)
}

func (em *errorMapping) SetInodeAttributes(
	ctx context.Context,
	op *fuseops.SetInodeAttributesOp) error {
	err := em.wrapped.SetInodeAttributes(ctx, op)
	return em.mapError(ctx, "SetInodeAttributes", err)
}

func (em *errorMapping) ForgetInode(
	ctx context.Context,
	op *fuseops.ForgetInodeOp) error {
	err := em.wrapped.ForgetInode(ctx, op)
	return em.mapError(ctx, "ForgetInode", err)
}

func (em *errorMapping) BatchForget(
	ctx context.Context,
	op *fuseops.BatchForgetOp) error {
	err := em.wrapped.BatchForget(ctx, op)
	return em.mapError(ctx, "BatchForget", err)
}

func (em *errorMapping) MkDir(
	ctx context.Context,
	op *fuseops.MkDirOp) error {
	err := em.wrapped.MkDir(ctx, op)
	return em.mapError(ctx, "MkDir", err)
}

func (em *errorMapping) MkNode(
	ctx context.Context,
	op *fuseops.MkNodeOp) error {
	err := em.wrapped.MkNode(ctx, op)
	return em.mapError(ctx, "MkNode", err)
}

func (em *errorMapping) CreateFile(
	ctx context.Context,
	op *fuseops.CreateFileOp) error {
	err := em.wrapped.CreateFile(ctx, op)
	return em.mapError(ctx, "CreateFile", err)
}

func (em *errorMapping) CreateLink(
	ctx context.Context,
	op *fuseops.CreateLinkOp) error {
	err := em.wrapped.CreateLink(ctx, op)
	return em.mapError(ctx, "CreateLink", err)
}

func (em *errorMapping) CreateSymlink(
	ctx context.Context,
	op *fuseops.CreateSymlinkOp) error {
	err := em.wrapped.CreateSymlink(ctx, op)
	return em.mapError(ctx, "CreateSymlink", err)
}

func (em *errorMapping) Rename(
	ctx context.Context,
	op *fuseops.RenameOp) error {
	err := em.wrapped.Rename(ctx, op)
	return em.mapError(ctx, "Rename", err)
}

func (em *errorMapping) RmDir(
	ctx context.Context,
	op *fuseops.RmDirOp) error {
	err := em.wrapped.RmDir(ctx, op)
	return em.mapError(ctx, "RmDir", err)
}

func (em *errorMapping) Unlink(
	ctx context.Context,
	op *fuseops.UnlinkOp) error {
	err := em.wrapped.Unlink(ctx, op)
	return em.mapError(ctx, "Unlink", err)
}

func (em *errorMapping) OpenDir(
	ctx context.Context,
	op *fuseops.OpenDirOp) error {
	err := em.wrapped.OpenDir(ctx, op)
	return em.mapError(ctx, "OpenDir", err)
}

func (em *errorMapping) ReadDir(
	ctx context.Context,
	op *fuseops.ReadDirOp) error {
	err := em.wrapped.ReadDir(ctx, op)
	return em.mapError(ctx, "ReadDir", err)
}

func (em *errorMapping) ReleaseDirHandle(
	ctx context.Context,
	op *fuseops.ReleaseDirHandleOp) error {
	err := em.wrapped.ReleaseDirHandle(ctx, op)
	return em.mapError(ctx, "ReleaseDirHandle", err)
}

func (em *errorMapping) OpenFile(
	ctx context.Context,
	op *fuseops.OpenFileOp) error {
	err := em.wrapped.OpenFile(ctx, op)
	return em.mapError(ctx, "OpenFile", err)
}

func (em *errorMapping) ReadFile(
	ctx context.Context,
	op *fuseops.ReadFileOp) error {
	err := em.wrapped.ReadFile(ctx, op)
	return em.mapError(ctx, "ReadFile", err)
}

func (em *errorMapping) WriteFile(
	ctx context.Context,
	op *fuseops.WriteFileOp) error {
	err := em.wrapped.WriteFile(ctx, op)
	return em.mapError(ctx, "WriteFile", err)
}

func (em *errorMapping) SyncFile(
	ctx context.Context,
	op *fuseops.SyncFileOp) error {
	err := em.wrapped.SyncFile(ctx, op)
	return em.mapError(ctx, "SyncFile", err)
}

func (em *errorMapping) FlushFile(
	ctx context.Context,
	op *fuseops.FlushFileOp) error {
	err := em.wrapped.FlushFile(ctx, op)
	return em.mapError(ctx, "FlushFile", err)
}

func (em *errorMapping) ReleaseFileHandle(
	ctx context.Context,
	op *fuseops.ReleaseFileHandleOp) error {
	err := em.wrapped.ReleaseFileHandle(ctx, op)
	return em.mapError(ctx, "ReleaseFileHandle", err)
}

func (em *errorMapping) ReadSymlink(
	ctx context.Context,
	op *fuseops.ReadSymlinkOp) error {
	err := em.wrapped.ReadSymlink(ctx, op)
	return em.mapError(ctx, "ReadSymlink", err)
}

func (em *errorMapping) RemoveXattr(
	ctx context.Context,
	op *fuseops.RemoveXattrOp) error {
	err := em.wrapped.RemoveXattr(ctx, op)
	return em.mapError(ctx, "RemoveXattr", err)
}

func (em *errorMapping) GetXattr(
	ctx context.Context,
	op *fuseops.GetXattrOp) error {
	err := em.wrapped.GetXattr(ctx, op)
	return em.mapError(ctx, "GetXattr", err)
}

func (em *errorMapping) ListXattr(
	ctx context.Context,
	op *fuseops.ListXattrOp) error {
	err := em.wrapped.ListXattr(ctx, op)
	return em.mapError(ctx, "ListXattr", err)
}

func (em *errorMapping) SetXattr(
	ctx context.Context,
	op *fuseops.SetXattrOp) error {
	err := em.wrapped.SetXattr(ctx, op)
	return em.mapError(ctx, "SetXattr", err)
}

func (em *errorMapping) Fallocate(
	ctx context.Context,
	op *fuseops.FallocateOp) error {
	err := em.wrapped.Fallocate(ctx, op)
	return em.mapError(ctx, "Fallocate", err)
}
