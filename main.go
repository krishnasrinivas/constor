package main

import (
	"fmt"
	"io"
	"log"
	"os"
	Path "path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/hanwen/go-fuse/fuse"
	// "github.com/VividCortex/godaemon"
)

const INOXATTR = "trusted.constor.ino"
const DELXATTR = "trusted.constor.deleted"

func Debug(format string, a ...interface{}) {
	return
	pc, file, line, _ := runtime.Caller(1)
	info := fmt.Sprintf(format, a...)
	funcName := runtime.FuncForPC(pc).Name()
	fmt.Printf("%s:%d:%s %v\n", Path.Base(file), line, funcName, info)
}

type Constor struct {
	sync.Mutex
	rootfs    string
	inodemap  *Inodemap
	dentrymap *Dentrymap
	fdmap     map[uintptr]*FD
	layers    []string
}

type FD struct {
	fd     int
	flags  int
	layer  int
	stream []DirEntry
}

func (constor *Constor) putfd(F *FD) {
	constor.Lock()
	defer constor.Unlock()
	ptr := uintptr(unsafe.Pointer(F))
	constor.fdmap[ptr] = F
}

func (constor *Constor) getfd(ptr uintptr) *FD {
	constor.Lock()
	defer constor.Unlock()
	F := constor.fdmap[ptr]
	return F
}

func (constor *Constor) deletefd(ptr uintptr) {
	constor.Lock()
	defer constor.Unlock()
	delete(constor.fdmap, ptr)
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
	Debug("%s", pathl)
	err := syscall.Lstat(pathl, stat)
	if err != nil {
		return err
	}
	if li != 0 {
		// INOXATTR valid only for layer-0
		return nil
	}
	var inobyte []byte
	inobyte = make([]byte, 100, 100)
	size, err := syscall.Getxattr(pathl, INOXATTR, inobyte)
	if err != nil {
		return nil
	}
	inostr := string(inobyte[:size])
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
	err := syscall.Lstat(pathl, stat)
	if err != nil {
		return err
	}
	if li != 0 {
		// INOXATTR valid only for layer-0
		return nil
	}

	var inobyte []byte
	inobyte = make([]byte, 100, 100)
	size, err := syscall.Getxattr(pathl, INOXATTR, inobyte)
	if err != nil {
		return nil
	}
	inostr := string(inobyte[:size])
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
	Debug("%s", dirpath)
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
	if err = syscall.Chown(dst, int(stat.Uid), int(stat.Gid)); err != nil {
		return err
	}
	if err = syscall.UtimesNano(dst, []syscall.Timespec{stat.Atim, stat.Mtim}); err != nil {
		return err
	}
	inoitoa := strconv.Itoa(int(stat.Ino))
	inobyte := []byte(inoitoa)
	if err = syscall.Setxattr(dst, INOXATTR, inobyte, 0); err != nil {
		return err
	}
	inode.layer = 0
	return nil
}

func (constor *Constor) Lookup(header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
	Debug("%d %s", header.NodeId, name)
	var stat syscall.Stat_t
	parent, err := constor.inodemap.findInode(header.NodeId)
	if err != nil {
		return fuse.ToStatus(err)
	}
	path, err := constor.dentrymap.getPathName(parent.ino, name)
	if err != nil {
		return fuse.ToStatus(err)
	}
	li := constor.getLayer(path)
	if li == -1 {
		return fuse.ENOENT
	}
	pathl := Path.Join(constor.layers[li], name)
	if constor.isdeleted(pathl) {
		return fuse.ENOENT
	}
	err = constor.Lstat(path, &stat)
	if err != nil {
		return fuse.ToStatus(err)
	}
	inode, err := constor.inodemap.findInode(stat.Ino)
	if err != nil {
		inode = NewInode(constor, stat.Ino)
		inode.mode = stat.Mode
		inode.layer = li
		constor.inodemap.hashInode(inode)
	} else {
		inode.layer = li
		inode.lookup()
	}
	if dentry, err := constor.dentrymap.findDentry(parent.ino, inode.ino); err != nil {
		dentry = new(Dentry)
		dentry.ino = inode.ino
		dentry.name = name
		dentry.parentino = parent.ino
		constor.dentrymap.hashDentry(dentry)
	}
	attr := (*fuse.Attr)(&out.Attr)
	attr.FromStat(&stat)
	out.NodeId = attr.Ino
	out.Ino = attr.Ino
	Debug("%d", out.Ino)
	return fuse.OK
}

func (constor *Constor) Forget(nodeID uint64, nlookup uint64) {
	Debug("%d %d", nodeID, nlookup)
	if inode, err := constor.inodemap.findInode(nodeID); err != nil {
		inode.forget(nlookup)
	}
}

func (constor *Constor) GetAttr(input *fuse.GetAttrIn, out *fuse.AttrOut) (code fuse.Status) {
	Debug("%d", input.NodeId)
	ino := input.NodeId
	stat := syscall.Stat_t{}
	path, err := constor.dentrymap.getPath(ino)
	if err != nil {
		return fuse.ToStatus(err)
	}
	Debug("%s", path)
	inode, err := constor.inodemap.findInode(ino)
	if err != nil {
		return fuse.ToStatus(err)
	}
	if err := constor.LstatInode(path, &stat, inode); err != nil {
		return fuse.ToStatus(err)
	}
	attr := (*fuse.Attr)(&out.Attr)
	attr.FromStat(&stat)
	if ino == 1 {
		attr.Ino = 1
	}
	Debug("%d", attr.Ino)
	return fuse.OK
}

func (constor *Constor) OpenDir(input *fuse.OpenIn, out *fuse.OpenOut) (code fuse.Status) {
	Debug("%d", input.NodeId)
	path, err := constor.dentrymap.getPath(input.NodeId)
	if err != nil {
		return fuse.ToStatus(err)
	}
	entries := map[string]DirEntry{}
	for li, layer := range constor.layers {
		f, err := os.Open(Path.Join(layer, path))
		if err != nil {
			continue
		}
		infos, _ := f.Readdir(0)
		for i := range infos {
			// workaround forhttps://code.google.com/p/go/issues/detail?id=5960
			if infos[i] == nil {
				continue
			}
			name := infos[i].Name()
			if _, ok := entries[name]; ok {
				// skip if the file was in upper layer
				continue
			}

			mode := infos[i].Mode()
			stat := infos[i].Sys().(*syscall.Stat_t)
			d := DirEntry{
				Name: name,
				Mode: uint32(mode),
				Ino:  stat.Ino,
			}
			if li == 0 {
				err := constor.Lstat(Path.Join(path, name), stat)
				if err == nil {
					d.Ino = stat.Ino
				}
			}
			d.Stat = *stat
			pathl := Path.Join(constor.layers[li], path, name)
			if constor.isdeleted(pathl) {
				d.Deleted = true
			}
			entries[name] = d
		}
		f.Close()
	}
	output := make([]DirEntry, 0, 500)

	for _, d := range entries {
		if d.Deleted {
			continue
		}
		output = append(output, d)
	}
	stat := syscall.Stat_t{}
	err = constor.Lstat(path, &stat)
	d := DirEntry{
		Name: ".",
		Mode: stat.Mode,
		Ino:  stat.Ino,
	}
	output = append(output, d)

	err = constor.Lstat(Path.Join(path, ".."), &stat)
	d = DirEntry{
		Name: "..",
		Mode: stat.Mode,
		Ino:  stat.Ino,
	}
	output = append(output, d)

	for i, _ := range output {
		output[i].Offset = uint64(i) + 1
	}
	F := new(FD)
	F.stream = output
	constor.putfd(F)
	out.Fh = uint64(uintptr(unsafe.Pointer(F)))
	out.OpenFlags = 0
	return fuse.OK
}

func (constor *Constor) ReadDir(input *fuse.ReadIn, fuseout *fuse.DirEntryList) fuse.Status {
	Debug("%d", input.Offset)
	ptr := uintptr(input.Fh)
	offset := input.Offset
	out := (*DirEntryList)(unsafe.Pointer(fuseout))

	F := constor.getfd(ptr)
	stream := F.stream
	if stream == nil {
		return fuse.EIO
	}
	if offset > uint64(len(stream)) {
		return fuse.EINVAL
	}
	todo := F.stream[offset:]
	for _, e := range todo {
		if e.Name == "" {
			log.Printf("got emtpy directory entry, mode %o.", e.Mode)
			continue
		}
		ok, _ := out.AddDirEntry(e)
		if !ok {
			break
		}
	}
	return fuse.OK
}

func (constor *Constor) ReleaseDir(input *fuse.ReleaseIn) {
	Debug("")
	ptr := uintptr(input.Fh)
	constor.deletefd(ptr)
}

func (constor *Constor) Init(*fuse.Server) {
}

func (constor *Constor) String() string {
	return os.Args[0]
}

func (constor *Constor) SetDebug(dbg bool) {
}

func (constor *Constor) StatFs(header *fuse.InHeader, out *fuse.StatfsOut) fuse.Status {
	Debug("%d", header.NodeId)
	path := constor.layers[0]
	s := syscall.Statfs_t{}
	err := syscall.Statfs(path, &s)
	if err == nil {
		out.Blocks = s.Blocks
		out.Bsize = uint32(s.Bsize)
		out.Bfree = s.Bfree
		out.Bavail = s.Bavail
		out.Files = s.Files
		out.Ffree = s.Ffree
		out.Frsize = uint32(s.Frsize)
		out.NameLen = uint32(s.Namelen)
		return fuse.OK
	} else {
		return fuse.ToStatus(err)
	}
}

func (constor *Constor) SetAttr(input *fuse.SetAttrIn, out *fuse.AttrOut) fuse.Status {
	Debug("%d %d", input.NodeId, input.Valid)
	var err error
	uid := -1
	gid := -1

	inode, err := constor.inodemap.findInode(input.NodeId)
	if err != nil {
		return fuse.ToStatus(err)
	}
	if inode.layer != 0 {
		err = constor.copyup(inode)
		if err != nil {
			Debug("%s", err)
			return fuse.ToStatus(err)
		}
	}

	stat := syscall.Stat_t{}
	path, err := constor.dentrymap.getPath(input.NodeId)
	if err != nil {
		Debug("%s", err)
		return fuse.ToStatus(err)
	}
	pathl := Path.Join(constor.layers[0], path)

	// just to satisfy PJD tests
	if input.Valid == 0 {
		err = os.Lchown(pathl, uid, gid)
		if err != nil {
			return fuse.ToStatus(err)
		}
	}
	if input.Valid&fuse.FATTR_MODE != 0 {
		permissions := uint32(07777) & input.Mode
		err = syscall.Chmod(pathl, permissions)
		if err != nil {
			return fuse.ToStatus(err)
		}
	}
	if input.Valid&(fuse.FATTR_UID) != 0 {
		uid = int(input.Uid)
	}
	if input.Valid&(fuse.FATTR_GID) != 0 {
		gid = int(input.Gid)
	}

	if input.Valid&(fuse.FATTR_UID|fuse.FATTR_GID) != 0 {
		Debug("%s %d %d", pathl, uid, gid)
		err = os.Lchown(pathl, uid, gid)
		if err != nil {
			return fuse.ToStatus(err)
		}
	}
	if input.Valid&fuse.FATTR_SIZE != 0 {
		err = os.Truncate(pathl, int64(input.Size))
		if err != nil {
			return fuse.ToStatus(err)
		}
	}
	if input.Valid&(fuse.FATTR_ATIME|fuse.FATTR_MTIME|fuse.FATTR_ATIME_NOW|fuse.FATTR_MTIME_NOW) != 0 {
		now := time.Now()
		var atime *time.Time
		var mtime *time.Time

		if input.Valid&fuse.FATTR_ATIME_NOW != 0 {
			atime = &now
		} else {
			t := time.Unix(int64(input.Atime), int64(input.Atimensec))
			atime = &t
		}

		if input.Valid&fuse.FATTR_MTIME_NOW != 0 {
			mtime = &now
		} else {
			t := time.Unix(int64(input.Mtime), int64(input.Mtimensec))
			mtime = &t
		}

		fi, err := os.Lstat(pathl)
		if err != nil {
			return fuse.ToStatus(err)
		}
		if fi.Mode()&os.ModeSymlink != os.ModeSymlink {
			// FIXME: there is no Lchtimes
			err = os.Chtimes(pathl, *atime, *mtime)
			if err != nil {
				Debug("%s", err)
				return fuse.ToStatus(err)
			}
		}
	}
	attr := (*fuse.Attr)(&out.Attr)

	err = syscall.Lstat(pathl, &stat)
	if err != nil {
		return fuse.ToStatus(err)
	}
	attr.FromStat(&stat)
	attr.Ino = input.NodeId
	return fuse.ToStatus(err)
}

func (constor *Constor) Readlink(header *fuse.InHeader) (out []byte, code fuse.Status) {
	Debug("%d", header.NodeId)
	pathl, err := constor.getPath(header.NodeId)
	if err != nil {
		return []byte{}, fuse.ToStatus(err)
	}
	link, err := os.Readlink(pathl)
	if err != nil {
		return []byte{}, fuse.ToStatus(err)
	}
	return []byte(link), fuse.OK
}

func (constor *Constor) Mknod(input *fuse.MknodIn, name string, out *fuse.EntryOut) (code fuse.Status) {
	path, err := constor.dentrymap.getPath(input.NodeId)
	if err != nil {
		return fuse.ToStatus(err)
	}
	if err = constor.createPath(path); err != nil {
		return fuse.ToStatus(err)
	}
	pathl := Path.Join(constor.layers[0], path, name)
	syscall.Unlink(pathl) // remove a deleted entry
	err = syscall.Mknod(pathl, input.Mode, int(input.Rdev))
	if err != nil {
		return fuse.ToStatus(err)
	}
	err = syscall.Chown(pathl, int(input.Uid), int(input.Gid))
	if err != nil {
		return fuse.ToStatus(err)
	}
	return constor.Lookup((*fuse.InHeader)(unsafe.Pointer(input)), name, out)
}

func (constor *Constor) Mkdir(input *fuse.MkdirIn, name string, out *fuse.EntryOut) (code fuse.Status) {
	Debug("%d %s", input.NodeId, name)
	path, err := constor.dentrymap.getPath(input.NodeId)
	if err != nil {
		return fuse.ToStatus(err)
	}
	if err := constor.createPath(path); err != nil {
		return fuse.ToStatus(err)
	}
	pathl := Path.Join(constor.layers[0], path, name)
	syscall.Unlink(pathl) // remove a deleted entry
	Debug("mkdir(%s)", pathl)
	err = syscall.Mkdir(pathl, input.Mode)
	if err != nil {
		return fuse.ToStatus(err)
	}
	err = syscall.Chown(pathl, int(input.Uid), int(input.Gid))
	if err != nil {
		return fuse.ToStatus(err)
	}
	return constor.Lookup((*fuse.InHeader)(unsafe.Pointer(input)), name, out)
}

func (constor *Constor) Unlink(header *fuse.InHeader, name string) (code fuse.Status) {
	Debug("%d %s", header.NodeId, name)
	var stat syscall.Stat_t
	parentino := header.NodeId
	dirpath, err := constor.dentrymap.getPath(parentino)
	if err != nil {
		return fuse.ToStatus(err)
	}
	path := Path.Join(dirpath, name)
	if err := constor.Lstat(path, &stat); err != nil {
		return fuse.ToStatus(err)
	}
	li := constor.getLayer(path)
	if li == -1 {
		return fuse.EIO
	}
	pathlayer0 := Path.Join(constor.layers[0], path)

	if li == 0 {
		err = syscall.Unlink(pathlayer0)
		if err != nil {
			return fuse.ToStatus(err)
		}
		li = constor.getLayer(path)
	}

	if li > 0 {
		err := constor.createPath(dirpath)
		if err != nil {
			return fuse.ToStatus(err)
		}
		fd, err := syscall.Creat(pathlayer0, 0)
		if err != nil {
			return fuse.ToStatus(err)
		}
		syscall.Close(fd)
		err = constor.setdeleted(pathlayer0)
		if err != nil {
			return fuse.ToStatus(err)
		}
	}

	dentry, err := constor.dentrymap.findDentry(parentino, stat.Ino)
	if err != nil {
		return fuse.ToStatus(err)
	}
	constor.dentrymap.unhashDentry(dentry)
	return fuse.OK
}

func (constor *Constor) Rmdir(header *fuse.InHeader, name string) (code fuse.Status) {
	Debug("%d %s", header.NodeId, name)
	var stat syscall.Stat_t
	parentino := header.NodeId
	dirpath, err := constor.dentrymap.getPath(parentino)
	if err != nil {
		return fuse.ToStatus(err)
	}
	path := Path.Join(dirpath, name)
	if err := constor.Lstat(path, &stat); err != nil {
		return fuse.ToStatus(err)
	}
	li := constor.getLayer(path)
	if li == -1 {
		return fuse.EIO
	}
	pathlayer0 := Path.Join(constor.layers[0], path)

	if li == 0 {
		// FIXME: make sure the dir is not empty
		err = os.RemoveAll(pathlayer0)
		if err != nil {
			return fuse.ToStatus(err)
		}
		li = constor.getLayer(path)
	}

	if li > 0 {
		err := constor.createPath(dirpath)
		if err != nil {
			return fuse.ToStatus(err)
		}
		fd, err := syscall.Creat(pathlayer0, 0)
		if err != nil {
			return fuse.ToStatus(err)
		}
		syscall.Close(fd)
		err = constor.setdeleted(pathlayer0)
		if err != nil {
			return fuse.ToStatus(err)
		}
	}

	dentry, err := constor.dentrymap.findDentry(parentino, stat.Ino)
	if err != nil {
		return fuse.ToStatus(err)
	}
	constor.dentrymap.unhashDentry(dentry)
	return fuse.OK
}

func (constor *Constor) Symlink(header *fuse.InHeader, pointedTo string, linkName string, out *fuse.EntryOut) (code fuse.Status) {
	Debug("%d %s <- %s, uid: %d, gid: %d", header.NodeId, pointedTo, linkName, header.Uid, header.Gid)
	parentino := header.NodeId
	path, err := constor.dentrymap.getPathName(parentino, linkName)
	if err != nil {
		return fuse.ToStatus(err)
	}
	pathl := Path.Join(constor.layers[0], path)
	syscall.Unlink(pathl) // remove a deleted entry
	err = syscall.Symlink(pointedTo, pathl)
	if err != nil {
		return fuse.ToStatus(err)
	}
	err = syscall.Lchown(pathl, int(header.Uid), int(header.Gid))
	if err != nil {
		return fuse.ToStatus(err)
	}
	return constor.Lookup(header, linkName, out)
}

func (constor *Constor) Rename(input *fuse.RenameIn, oldName string, newName string) (code fuse.Status) {
	var unhashDentry *Dentry
	oldParent := input.NodeId
	newParent := input.Newdir

	oldpath, err := constor.dentrymap.getPathName(oldParent, oldName)
	if err != nil {
		return fuse.ToStatus(err)
	}
	newpath, err := constor.dentrymap.getPathName(newParent, newName)
	if err != nil {
		return fuse.ToStatus(err)
	}

	// remove dst file
	oldli := constor.getLayer(oldpath)

	if oldli == -1 {
		return fuse.ENOENT
	}

	oldstat := syscall.Stat_t{}
	newstat := syscall.Stat_t{}

	err = constor.Lstat(oldpath, &oldstat)
	if err != nil {
		return fuse.ToStatus(err)
	}
	if oldstat.Mode|syscall.S_IFDIR != 0 {
		// FIXME: allow renaming of directories
		return fuse.EIO
	}

	err = constor.Lstat(newpath, &newstat)
	if err == nil {
		unhashDentry, _ = constor.dentrymap.findDentry(newParent, newstat.Ino)
	}

	oldpathl := Path.Join(constor.layers[0], oldpath)
	newpathl := Path.Join(constor.layers[0], newpath)

	if oldli == 0 {
		err = syscall.Rename(oldpathl, newpathl)
		if err != nil {
			return fuse.ToStatus(err)
		}
		lowerli := constor.getLayer(oldpath)
		if lowerli != -1 {
			constor.setdeleted(oldpathl)
		}
	} else if oldli > 0 {
		inode, err := constor.inodemap.findInode(oldstat.Ino)
		if err != nil {
			return fuse.ToStatus(err)
		}
		err = constor.copyup(inode)
		if err != nil {
			return fuse.ToStatus(err)
		}
		err = syscall.Rename(oldpathl, newpathl)
		if err != nil {
			return fuse.ToStatus(err)
		}
		constor.setdeleted(oldpathl)
	}
	if unhashDentry != nil {
		constor.dentrymap.unhashDentry(unhashDentry)
	}

	err = constor.dentrymap.dentryChangeparent(oldstat.Ino, oldParent, newParent, oldName, newName)
	if err != nil {
		return fuse.ToStatus(err)
	}
	return fuse.OK
}

func (constor *Constor) Link(input *fuse.LinkIn, name string, out *fuse.EntryOut) (code fuse.Status) {
	Debug("%d %d %s", input.Oldnodeid, input.NodeId, name)
	oldpath, err := constor.dentrymap.getPath(input.Oldnodeid)
	if err != nil {
		return fuse.ToStatus(err)
	}
	li := constor.getLayer(oldpath)
	if li != 0 {
		return fuse.EIO
	}
	newpath, err := constor.dentrymap.getPathName(input.NodeId, name)
	if err != nil {
		return fuse.ToStatus(err)
	}
	oldpathl := Path.Join(constor.layers[0], oldpath)
	newpathl := Path.Join(constor.layers[0], newpath)
	err = syscall.Link(oldpathl, newpathl)
	if err != nil {
		return fuse.ToStatus(err)
	}
	return constor.Lookup((*fuse.InHeader)(unsafe.Pointer(input)), name, out)
}

func (constor *Constor) GetXAttrSize(header *fuse.InHeader, attr string) (size int, code fuse.Status) {
	return 0, fuse.ENOSYS
}

func (constor *Constor) GetXAttrData(header *fuse.InHeader, attr string) (data []byte, code fuse.Status) {
	return nil, fuse.ENOSYS
}

func (constor *Constor) SetXAttr(input *fuse.SetXAttrIn, attr string, data []byte) fuse.Status {
	return fuse.ENOSYS
}

func (constor *Constor) ListXAttr(header *fuse.InHeader) (data []byte, code fuse.Status) {
	return nil, fuse.ENOSYS
}

func (constor *Constor) RemoveXAttr(header *fuse.InHeader, attr string) fuse.Status {
	if attr == "inodemap" {
		fmt.Println(constor.inodemap)
	}
	if attr == "dentrymap" {
		fmt.Println(constor.dentrymap)
	}
	return fuse.OK
}

func (constor *Constor) Access(input *fuse.AccessIn) (code fuse.Status) {
	Debug("%d", input.NodeId)
	// FIXME: oops fix this
	path, err := constor.getPath(input.NodeId)
	if err != nil {
		return fuse.ToStatus(err)
	}
	return fuse.ToStatus(syscall.Access(path, input.Mask))
}

func (constor *Constor) Create(input *fuse.CreateIn, name string, out *fuse.CreateOut) (code fuse.Status) {
	dirpath, err := constor.dentrymap.getPath(input.NodeId)
	if err != nil {
		return fuse.ToStatus(err)
	}
	Debug("%s%s %d %d %d", dirpath, name, input.Mode, input.Uid, input.Gid)
	if err := constor.createPath(dirpath); err != nil {
		return fuse.ToStatus(err)
	}
	pathl := Path.Join(constor.layers[0], dirpath, name)
	// remove any deleted place holder entries
	syscall.Unlink(pathl)
	fd, err := syscall.Creat(pathl, input.Mode)
	if err != nil {
		return fuse.ToStatus(err)
	}

	err = syscall.Chown(pathl, int(input.Uid), int(input.Gid))
	if err != nil {
		return fuse.ToStatus(err)
	}
	F := new(FD)
	F.fd = fd
	F.layer = 0
	constor.putfd(F)
	out.Fh = uint64(uintptr(unsafe.Pointer(F)))
	return constor.Lookup((*fuse.InHeader)(unsafe.Pointer(input)), name, &out.EntryOut)
}

func (constor *Constor) Open(input *fuse.OpenIn, out *fuse.OpenOut) (status fuse.Status) {
	pathl, err := constor.getPath(input.NodeId)
	if err != nil {
		return fuse.ToStatus(err)
	}
	inode, err := constor.inodemap.findInode(input.NodeId)
	if err != nil {
		return fuse.ToStatus(err)
	}
	Debug("%s %d %d %d", pathl, input.Flags, input.Uid, input.Gid)
	fd, err := syscall.Open(pathl, int(input.Flags), 0)
	if err != nil {
		return fuse.ToStatus(err)
	}
	F := new(FD)
	F.fd = fd
	F.flags = int(input.Flags)
	F.layer = inode.layer
	constor.putfd(F)
	out.Fh = uint64(uintptr(unsafe.Pointer(F)))
	out.OpenFlags = 0
	return fuse.OK
}

func (constor *Constor) Read(input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	Debug("%d", input.Fh)
	ptr := uintptr(input.Fh)
	inode, err := constor.inodemap.findInode(input.NodeId)
	if err != nil {
		return nil, fuse.ToStatus(err)
	}
	offset := input.Offset

	F := constor.getfd(ptr)

	if F == nil {
		return nil, fuse.EIO
	}

	if (F.layer != inode.layer) && (inode.layer == 0) {
		syscall.Close(F.fd)
		pathl, err := constor.getPath(input.NodeId)
		if err != nil {
			return nil, fuse.ToStatus(err)
		}
		fd, err := syscall.Open(pathl, F.flags, 0)
		if err != nil {
			return nil, fuse.ToStatus(err)
		}
		F.fd = fd
		F.layer = 0
	}

	fd := F.fd
	_, err = syscall.Pread(fd, buf, int64(offset))
	return nil, fuse.ToStatus(err)
}

func (constor *Constor) Release(input *fuse.ReleaseIn) {
	ptr := uintptr(input.Fh)
	F := constor.getfd(ptr)
	if F == nil {
		return
	}
	fd := F.fd
	constor.deletefd(ptr)
	syscall.Close(fd)
}

func (constor *Constor) Write(input *fuse.WriteIn, data []byte) (written uint32, code fuse.Status) {
	Debug("%d", input.Fh)
	ptr := uintptr(input.Fh)
	offset := input.Offset

	F := constor.getfd(ptr)
	if F == nil {
		return 0, fuse.EIO
	}
	if F.layer != 0 {
		inode, err := constor.inodemap.findInode(input.NodeId)
		if err != nil {
			return 0, fuse.ToStatus(err)
		}
		err = constor.copyup(inode)
		if err != nil {
			return 0, fuse.ToStatus(err)
		}
		path, err := constor.dentrymap.getPath(inode.ino)
		if err != nil {
			return 0, fuse.ToStatus(err)
		}
		pathl := Path.Join(constor.layers[0], path)
		syscall.Close(F.fd)
		fd, err := syscall.Open(pathl, F.flags, 0)
		if err != nil {
			return 0, fuse.ToStatus(err)
		}
		F.fd = fd
		F.layer = 0
	}

	fd := F.fd
	n, err := syscall.Pwrite(fd, data, int64(offset))
	return uint32(n), fuse.ToStatus(err)
}

func (constor *Constor) Flush(input *fuse.FlushIn) fuse.Status {
	Debug("")
	return fuse.OK
}

func (constor *Constor) Fsync(input *fuse.FsyncIn) (code fuse.Status) {
	Debug("")
	return fuse.OK
}

func (constor *Constor) ReadDirPlus(input *fuse.ReadIn, fuseout *fuse.DirEntryList) fuse.Status {
	Debug("")
	Debug("%d", input.Offset)
	ptr := uintptr(input.Fh)
	offset := input.Offset
	entryOut := fuse.EntryOut{}
	out := (*DirEntryList)(unsafe.Pointer(fuseout))

	F := constor.getfd(ptr)
	stream := F.stream
	if stream == nil {
		return fuse.EIO
	}
	if offset > uint64(len(stream)) {
		return fuse.EINVAL
	}
	todo := F.stream[offset:]
	for _, e := range todo {
		if e.Name == "" {
			log.Printf("got emtpy directory entry, mode %o.", e.Mode)
			continue
		}
		attr := (*fuse.Attr)(&entryOut.Attr)
		attr.FromStat(&e.Stat)
		entryOut.NodeId = attr.Ino
		entryOut.Ino = attr.Ino
		ok, _ := out.AddDirLookupEntry(e, &entryOut)
		if !ok {
			break
		}
	}
	return fuse.OK
}

func (constor *Constor) FsyncDir(input *fuse.FsyncIn) (code fuse.Status) {
	return fuse.OK
}

func (constor *Constor) Fallocate(in *fuse.FallocateIn) (code fuse.Status) {
	return fuse.ENOSYS
}

func main() {
	// godaemon.MakeDaemon(&godaemon.DaemonAttr{})
	log.SetFlags(log.Lshortfile)

	if len(os.Args) == 1 {
		fmt.Println("Usage: constor /layer0:/layer1:....:/layerN /mnt/point")
		os.Exit(1)
	}

	layers := os.Args[1]
	mountPoint := os.Args[2]

	// log := strings.Split(mountPoint, "/")
	// pid := os.Getpid()
	// pidstr := strconv.Itoa(pid)
	// logfd, err := syscall.Open("/tmp/constor.log." + pidstr + "." + log[len(log)-1], syscall.O_RDWR|syscall.O_CREAT, 0)

	constor := new(Constor)
	constor.inodemap = NewInodemap(constor)
	constor.dentrymap = NewDentrymap(constor)
	constor.fdmap = make(map[uintptr]*FD)

	constor.layers = strings.Split(layers, ":")

	constor.rootfs = constor.layers[0]
	mOpts := &fuse.MountOptions{
		Name:    "constor",
		Options: []string{"nonempty", "allow_other", "default_permissions", "user_id=0", "group_id=0", "fsname=" + constor.layers[0]},
	}
	_ = syscall.Umask(000)

	state, err := fuse.NewServer(constor, mountPoint, mOpts)
	if err != nil {
		fmt.Printf("Mount fail: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Mounted!")
	state.Serve()
}
