package driver

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

const (
	// localCopySentinel is a hidden file created at the root of the local copy
	// directory once the initial copy from the volume has completed. Its name
	// is intentionally unlikely to collide with user data.
	localCopySentinel = ".scaleway-csi-copy-done"

	// LocalCopyMntDirEnv overrides the base directory used to mount volumes
	// internally when unsafe-local-copy mode is enabled.
	LocalCopyMntDirEnv = "LOCAL_COPY_MNT_DIR"
	// LocalCopyDataDirEnv overrides the base directory used to store the local
	// copy of volume data when unsafe-local-copy mode is enabled.
	LocalCopyDataDirEnv = "LOCAL_COPY_DATA_DIR"

	defaultLocalCopyMntDir  = "/var/lib/scaleway-csi/mnt"
	defaultLocalCopyDataDir = "/var/lib/scaleway-csi/copy"
)

// localCopyDirs returns the base directories to use for the internal mount
// point and the local copy, reading overrides from environment variables.
func localCopyDirs() (mntDir, dataDir string) {
	mntDir = defaultLocalCopyMntDir
	if v := os.Getenv(LocalCopyMntDirEnv); v != "" {
		mntDir = v
	}
	dataDir = defaultLocalCopyDataDir
	if v := os.Getenv(LocalCopyDataDirEnv); v != "" {
		dataDir = v
	}
	return
}

// stageLocalCopy mounts the volume to an internal path, copies its data to a
// local directory, clears the volume, and bind-mounts the local copy to
// stagingTargetPath.
func (d *nodeService) stageLocalCopy(volumeID, volumeName, stagingTargetPath, devicePath, fsType string, mountOptions []string) (*csi.NodeStageVolumeResponse, error) {
	internalStagingPath := filepath.Join(d.localCopyMntDir, volumeID)

	if err := createMountPoint(internalStagingPath, false); err != nil {
		return nil, status.Errorf(codes.Internal, "error creating mount point %s for volume with ID %s", internalStagingPath, volumeID)
	}

	// format and mount volume to internal path
	if !d.diskUtils.IsMounted(internalStagingPath) {
		if err := d.diskUtils.FormatAndMount(internalStagingPath, devicePath, fsType, mountOptions); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to format and mount device from (%q) to (%q) with fstype (%q) and options (%q): %s",
				devicePath, internalStagingPath, fsType, mountOptions, err)
		}
	}

	klog.V(4).Infof("Volume %s with ID %s has been mounted on %s with type %s and options %s", volumeName, volumeID, internalStagingPath, fsType, mountOptions)

	// Try expanding the volume if it's created from a snapshot.
	if err := d.diskUtils.Resize(internalStagingPath, devicePath, ""); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to resize volume: %s", err)
	}

	// Create local copy folder.
	localCopyPath := filepath.Join(d.localCopyDataDir, volumeID)
	if err := os.MkdirAll(localCopyPath, 0755); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create local copy folder: %s", err)
	}

	// Copy from volume to local copy dir (if not already done).
	doneFilename := filepath.Join(localCopyPath, localCopySentinel)
	if _, err := os.Stat(doneFilename); errors.Is(err, fs.ErrNotExist) {
		// Clear any partial previous attempt, including dotfiles.
		if err := clearDir(localCopyPath); err != nil {
			return nil, err
		}

		// Verify enough space is available before copying.
		if err := checkDiskSpace(internalStagingPath, d.localCopyDataDir); err != nil {
			return nil, status.Errorf(codes.ResourceExhausted, "insufficient disk space for local copy of volume %s: %s", volumeID, err)
		}

		if err := copyDir(internalStagingPath, localCopyPath); err != nil {
			return nil, fmt.Errorf("failed to copy volume data to local copy: %w", err)
		}

		file, err := os.Create(doneFilename)
		if err != nil {
			return nil, err
		}
		if err := file.Close(); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	// Ensure volume is empty to avoid serving stale data on restart.
	if err := clearDir(internalStagingPath); err != nil {
		return nil, err
	}

	if !d.diskUtils.IsMounted(stagingTargetPath) {
		// Bind mount local copy to staging target.
		if err := createMountPoint(stagingTargetPath, false); err != nil {
			return nil, status.Errorf(codes.Internal, "error creating mount point %s for volume with ID %s", stagingTargetPath, volumeID)
		}

		if err := d.diskUtils.MountToTarget(localCopyPath, stagingTargetPath, fsType, []string{"bind"}); err != nil {
			return nil, status.Errorf(codes.Internal, "error mounting source %s to target %s with fs of type %s : %s", localCopyPath, stagingTargetPath, fsType, err.Error())
		}
	}

	return &csi.NodeStageVolumeResponse{}, nil
}

// unstageLocalCopy writes back the local copy to the volume, unmounts both
// mount points, and removes the local copy directory.
func (d *nodeService) unstageLocalCopy(volumeID, stagingTargetPath string) error {
	internalStagingPath := filepath.Join(d.localCopyMntDir, volumeID)
	localCopyPath := filepath.Join(d.localCopyDataDir, volumeID)

	doneFilename := filepath.Join(localCopyPath, localCopySentinel)
	if _, err := os.Stat(doneFilename); err == nil {
		if !d.diskUtils.IsMounted(internalStagingPath) {
			return status.Errorf(codes.Internal, "volume with ID %s is not mounted on %s, cannot write back local copy", volumeID, internalStagingPath)
		}

		// Clear the volume before writing back, including dotfiles.
		if err := clearDir(internalStagingPath); err != nil {
			return err
		}

		// Copy local copy back to volume, skipping the sentinel file.
		if err := copyDir(localCopyPath, internalStagingPath, localCopySentinel); err != nil {
			return fmt.Errorf("failed to copy local copy data back to volume: %w", err)
		}

		// Remove sentinel after a successful copy-back so that a retry after
		// a failed unmount does not copy again unnecessarily.
		if err := os.Remove(doneFilename); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("failed to remove sentinel file: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("error during stat of local copy dir: %w", err)
	}

	// Unmount bind mount before deleting the local copy dir.
	if d.diskUtils.IsMounted(stagingTargetPath) {
		klog.V(4).Infof("Volume with ID %s is bind-mounted on %s, unmounting it", volumeID, stagingTargetPath)
		if err := d.diskUtils.Unmount(stagingTargetPath); err != nil {
			return status.Errorf(codes.Internal, "error unmounting target path: %s", err.Error())
		}
	}

	if d.diskUtils.IsMounted(internalStagingPath) {
		klog.V(4).Infof("Volume with ID %s is mounted on %s, unmounting it", volumeID, internalStagingPath)
		if err := d.diskUtils.Unmount(internalStagingPath); err != nil {
			return status.Errorf(codes.Internal, "error unmounting internal path: %s", err.Error())
		}
	}

	// Delete local copy dir.
	return os.RemoveAll(localCopyPath)
}
