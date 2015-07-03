package main

import (
	"sync"
	"syscall"
)

type Inode struct {
	nlookup uint32
	mode    uint32
	ino     uint64
	layer   int
	sync.Mutex
	constor *Constor
}

func (inode *Inode) lookup() {
	inode.Lock()
	defer inode.Unlock()
	inode.nlookup++
}

func (inode *Inode) forget(n uint64) {
	inode.Lock()
	defer inode.Unlock()
	inode.nlookup -= uint32(n)
	if n == 0 {
		inode.nlookup = 0
	}
	if inode.nlookup == 0 {
		inode.constor.inodemap.unhashInode(inode)
		delete(inode.constor.dentrymap.dmap, inode.ino)
	}
}

func NewInode(constor *Constor, ino uint64) *Inode {
	inode := new(Inode)
	inode.constor = constor
	inode.ino = ino
	inode.nlookup = 1
	inode.layer = -1
	return inode
}

type Inodemap struct {
	imap    map[uint64]*Inode
	constor *Constor
}

func NewInodemap(constor *Constor) *Inodemap {
	inodemap := new(Inodemap)
	inodemap.constor = constor
	inodemap.imap = make(map[uint64]*Inode)

	inode := NewInode(constor, 1)
	inodemap.hashInode(inode)
	return inodemap
}

func (inodemap *Inodemap) findInode(ino uint64) (*Inode, error) {
	inodemap.constor.Lock()
	defer inodemap.constor.Unlock()
	inode, ok := inodemap.imap[ino]
	if ok {
		return inode, nil
	}
	return nil, syscall.ENOENT
}

func (inodemap *Inodemap) hashInode(inode *Inode) {
	inodemap.constor.Lock()
	defer inodemap.constor.Unlock()
	inodemap.imap[inode.ino] = inode
}

func (inodemap *Inodemap) unhashInode(inode *Inode) {
	inodemap.constor.Lock()
	defer inodemap.constor.Unlock()
	if inodemap.imap[inode.ino] == inode {
		delete(inodemap.imap, inode.ino)
	}
}
