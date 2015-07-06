package main

import (
	"fmt"
	"io"
	"os"
	Path "path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"unsafe"
)

type Constor struct {
	sync.Mutex
	rootfs    string
	logf	  *os.File
	inodemap  *Inodemap
	dentrymap *Dentrymap
	fdmap     map[uintptr]*FD
	layers    []string
}

func (constor *Constor) log(format string, a ...interface{}) {
	pc, file, line, _ := runtime.Caller(1)
	info := fmt.Sprintf(format, a...)
	funcName := runtime.FuncForPC(pc).Name()
	fmt.Fprintf(constor.logf, "%s:%d:%s %v\n", Path.Base(file), line, funcName, info)
}

func (constor *Constor) error(format string, a ...interface{}) {
	pc, file, line, _ := runtime.Caller(1)
	info := fmt.Sprintf(format, a...)
	funcName := runtime.FuncForPC(pc).Name()
	fmt.Fprintf(constor.logf, "ERR %s:%d:%s %v\n", Path.Base(file), line, funcName, info)
}

func Lgetxattr(path string, attr string) ([]byte, error) {
	pathBytes, err := syscall.BytePtrFromString(path)
	if err != nil {
		return nil, err
	}
	attrBytes, err := syscall.BytePtrFromString(attr)
	if err != nil {
		return nil, err
	}
	dest := make([]byte, 128)
	destBytes := unsafe.Pointer(&dest[0])
	sz, _, errno := syscall.Syscall6(syscall.SYS_LGETXATTR, uintptr(unsafe.Pointer(pathBytes)), uintptr(unsafe.Pointer(attrBytes)), uintptr(destBytes), uintptr(len(dest)), 0, 0)
	if errno == syscall.ENODATA {
		return nil, nil
	}
	if errno == syscall.ERANGE {
		dest = make([]byte, sz)
		destBytes := unsafe.Pointer(&dest[0])
		sz, _, errno = syscall.Syscall6(syscall.SYS_LGETXATTR, uintptr(unsafe.Pointer(pathBytes)), uintptr(unsafe.Pointer(attrBytes)), uintptr(destBytes), uintptr(len(dest)), 0, 0)
	}
	if errno != 0 {
		return nil, errno
	}

	return dest[:sz], nil
}

var _zero uintptr

func Lsetxattr(path string, attr string, data []byte, flags int) error {
	pathBytes, err := syscall.BytePtrFromString(path)
	if err != nil {
		return err
	}
	attrBytes, err := syscall.BytePtrFromString(attr)
	if err != nil {
		return err
	}
	var dataBytes unsafe.Pointer
	if len(data) > 0 {
		dataBytes = unsafe.Pointer(&data[0])
	} else {
		dataBytes = unsafe.Pointer(&_zero)
	}
	_, _, errno := syscall.Syscall6(syscall.SYS_LSETXATTR, uintptr(unsafe.Pointer(pathBytes)), uintptr(unsafe.Pointer(attrBytes)), uintptr(dataBytes), uintptr(len(data)), uintptr(flags), 0)
	if errno != 0 {
		return errno
	}
	return nil
}

func (constor *Constor) getLayer(path string) int {
	for i, l := range constor.layers {
		pathl := Path.Join(l, path)
		if constor.isdeleted(pathl) {
			return -1
		}
		if _, err := os.Lstat(pathl); err == nil {
			return i
		}
	}
	return -1
}

func (constor *Constor) getPath(ino uint64) (string, error) {
	path, err := constor.dentrymap.getPath(ino)
	if err != nil {
		return path, err
	}
	inode, err := constor.inodemap.findInode(ino)
	if err != nil {
		return "", err
	}
	if inode.layer != -1 {
		return Path.Join(constor.layers[inode.layer], path), nil
	}
	li := constor.getLayer(path)
	if li == -1 {
		return "", syscall.ENOENT
	}
	inode.layer = li
	return Path.Join(constor.layers[li], path), nil
}

func (constor *Constor) getPathName(ino uint64, name string) (string, error) {
	path, err := constor.getPath(ino)
	if err != nil {
		return "", err
	}
	return Path.Join(path, name), nil
}

func (constor *Constor) LstatInode(path string, stat *syscall.Stat_t, inode *Inode) error {
	li := inode.layer
	if li == -1 {
		li = constor.getLayer(path)
		if li == -1 {
			return syscall.ENOENT
		}
		inode.layer = li
	}
	pathl := Path.Join(constor.layers[li], path)
	err := syscall.Lstat(pathl, stat)
	if err != nil {
		return err
	}
	if li != 0 {
		// INOXATTR valid only for layer-0
		return nil
	}
	// var inobyte []byte
	// inobyte = make([]byte, 100, 100)
	// size, err := syscall.Getxattr(pathl, INOXATTR, inobyte)
	inobyte, err := Lgetxattr(pathl, INOXATTR)
	if err != nil {
		return nil
	}
	if len(inobyte) == 0 {
		return nil
	}
	inostr := string(inobyte)
	ino, err := strconv.Atoi(inostr)
	if err != nil {
		return err
	}
	stat.Ino = uint64(ino)
	if inode.ino == 1 {
		stat.Ino = 1
	}
	return nil
}

func (constor *Constor) Lstat(path string, stat *syscall.Stat_t) error {
	li := constor.getLayer(path)
	if li == -1 {
		return syscall.ENOENT
	}
	pathl := Path.Join(constor.layers[li], path)
	constor.log("%s", pathl)
	err := syscall.Lstat(pathl, stat)
	if err != nil {
		return err
	}
	if li != 0 {
		// INOXATTR valid only for layer-0
		return nil
	}

	// var inobyte []byte
	// inobyte = make([]byte, 100, 100)
	// size, err := syscall.Getxattr(pathl, INOXATTR, inobyte)
	inobyte, err := Lgetxattr(pathl, INOXATTR)
	if err != nil {
		return nil
	}
	if len(inobyte) == 0 {
		return nil
	}
	inostr := string(inobyte)
	ino, err := strconv.Atoi(inostr)
	if err != nil {
		return err
	}
	stat.Ino = uint64(ino)
	if path == "/" {
		stat.Ino = 1
	}
	return nil
}

func (constor *Constor) createPath(dirpath string) error {
	dirs := strings.Split(dirpath, "/")
	if len(dirs) == 0 {
		return syscall.EIO
	}
	subdir := ""
	for _, dir := range dirs {
		if dir == "" {
			continue
		}
		subdir = Path.Join(subdir, "/", dir)
		li := constor.getLayer(subdir)
		if li == 0 {
			continue
		}
		if li == -1 {
			return syscall.EIO
		}
		stat := syscall.Stat_t{}
		if err := constor.Lstat(subdir, &stat); err != nil {
			return err
		}
		subdirl := Path.Join(constor.layers[0], subdir)
		if err := syscall.Mkdir(subdirl, stat.Mode); err != nil {
			return err
		}
		if err := syscall.Chown(subdirl, int(stat.Uid), int(stat.Gid)); err != nil {
			return err
		}
		if err := syscall.UtimesNano(subdirl, []syscall.Timespec{stat.Atim, stat.Mtim}); err != nil {
			return err
		}
		inoitoa := strconv.Itoa(int(stat.Ino))
		inobyte := []byte(inoitoa)
		if err := syscall.Setxattr(subdirl, INOXATTR, inobyte, 0); err != nil {
			return err
		}
		inode, err := constor.inodemap.findInode(stat.Ino)
		if err != nil {
			return err
		}
		inode.Lock()
		inode.layer = 0
		inode.Unlock()
	}
	return nil
}

func (constor *Constor) setdeleted(pathl string) error {
	stat := syscall.Stat_t{}
	err := syscall.Stat(pathl, &stat)
	if err != nil {
		fd, err := syscall.Creat(pathl, 0)
		if err != nil {
			return err
		}
		syscall.Close(fd)
	}
	return syscall.Setxattr(pathl, DELXATTR, []byte{49}, 0)
}

func (constor *Constor) isdeleted(pathl string) bool {
	var inobyte []byte
	inobyte = make([]byte, 100, 100)
	if _, err := syscall.Getxattr(pathl, DELXATTR, inobyte); err == nil {
		return true
	} else {
		return false
	}
}

func (constor *Constor) copyup(inode *Inode) error {
	src, err := constor.getPath(inode.ino)
	if err != nil {
		return err
	}
	dst, err := constor.dentrymap.getPath(inode.ino)
	if err != nil {
		return err
	}
	err = constor.createPath(Path.Dir(dst))
	if err != nil {
		return err
	}
	dst = Path.Join(constor.layers[0], dst)
	fi, err := os.Lstat(src)
	if err != nil {
		return err
	}
	if fi.Mode()&os.ModeSymlink == os.ModeSymlink {
		linkName, err := os.Readlink(src)
		if err != nil {
			return err
		}
		err = os.Symlink(linkName, dst)
		if err != nil {
			return err
		}
	} else if fi.Mode()&os.ModeDir == os.ModeDir {
		err := os.Mkdir(dst, fi.Mode())
		if err != nil {
			return err
		}
	} else {
		in, err := os.Open(src)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.Create(dst)
		if err != nil {
			return err
		}
		defer out.Close()
		_, err = io.Copy(out, in)
		if err != nil {
			return err
		}
		err = out.Close()
		if err != nil {
			return err
		}
	}
	stat := syscall.Stat_t{}
	if err = syscall.Lstat(src, &stat); err != nil {
		return err
	}
	if fi.Mode()&os.ModeSymlink != os.ModeSymlink {
		if err = syscall.Chmod(dst, stat.Mode); err != nil {
			return err
		}
	}
	if err = syscall.Lchown(dst, int(stat.Uid), int(stat.Gid)); err != nil {
		return err
	}
	if fi.Mode()&os.ModeSymlink != os.ModeSymlink {
		if err = syscall.UtimesNano(dst, []syscall.Timespec{stat.Atim, stat.Mtim}); err != nil {
			return err
		}
	}
	inoitoa := strconv.Itoa(int(stat.Ino))
	inobyte := []byte(inoitoa)
	// if err = syscall.Setxattr(dst, INOXATTR, inobyte, 0); err != nil {
	// 	return err
	// }
	if err = Lsetxattr(dst, INOXATTR, inobyte, 0); err != nil {
		return err
	}
	inode.layer = 0
	path, err := constor.dentrymap.getPath(inode.ino)
	constor.log("ino %d file %s", inode.ino, path)
	return nil
}
