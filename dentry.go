package main

import (
	Path "path"
	"syscall"
)

type Dentry struct {
	parentino uint64
	name      string
	ino       uint64
}

type Dentrymap struct {
	dmap    map[uint64][]*Dentry
	constor *Constor
}

func NewDentrymap(constor *Constor) *Dentrymap {
	dmap := new(Dentrymap)
	dmap.dmap = make(map[uint64][]*Dentry)
	dmap.constor = constor
	return dmap
}

func (dentrymap *Dentrymap) findDentry(parentino uint64, ino uint64) (*Dentry, error) {
	dentrymap.constor.Lock()
	defer dentrymap.constor.Unlock()
	darray, ok := dentrymap.dmap[ino]
	if !ok {
		return nil, syscall.ENOENT
	}
	for _, d := range darray {
		if d.parentino == parentino {
			return d, nil
		}
	}
	return nil, syscall.ENOENT
}

func (dentrymap *Dentrymap) hashDentry(dentry *Dentry) {
	dentrymap.constor.Lock()
	defer dentrymap.constor.Unlock()

	if dentrymap.dmap[dentry.ino] == nil {
		dentrymap.dmap[dentry.ino] = make([]*Dentry, 0)
	}
	arr := dentrymap.dmap[dentry.ino]
	dentrymap.dmap[dentry.ino] = append(arr, dentry)
}

func (dentrymap *Dentrymap) unhashDentry(dentry *Dentry) {
	dentrymap.constor.Lock()
	defer dentrymap.constor.Unlock()
	m, ok := dentrymap.dmap[dentry.ino]
	if !ok {
		return
	}
	l := len(m)
	for i, d := range m {
		if d == dentry {
			m[i] = m[l-1]
			dentrymap.dmap[dentry.ino] = m[0 : l-1]
			break
		}
	}
	if len(dentrymap.dmap[dentry.ino]) == 0 {
		delete(dentrymap.dmap, dentry.ino)
	}
}

func (dentrymap *Dentrymap) dentryChangeparent(ino uint64, oldParent uint64, newParent uint64, oldName string, newName string) error {
	dentrymap.constor.Lock()
	defer dentrymap.constor.Unlock()
	m, ok := dentrymap.dmap[ino]
	if !ok {
		return syscall.ENOENT
	}
	for _, d := range m {
		if d.parentino == oldParent && d.name == oldName {
			d.parentino = newParent
			d.name = newName
		}
	}
	return nil
}

func (dentrymap *Dentrymap) getPath(ino uint64) (string, error) {
	dentrymap.constor.Lock()
	defer dentrymap.constor.Unlock()

	path := ""
	if ino == 1 {
		return "/", nil
	}
	for {
		darray, ok := dentrymap.dmap[ino]
		if !ok {
			return "", syscall.ENOENT
		}
		d := darray[0]
		path = Path.Join("/", d.name, path)
		ino = d.parentino
		if ino == 1 {
			break
		}
	}
	return path, nil
}

func (dentrymap *Dentrymap) getPathName(parentino uint64, name string) (string, error) {
	path, err := dentrymap.getPath(parentino)
	if err != nil {
		return path, err
	}
	return Path.Join(path, name), nil
}
