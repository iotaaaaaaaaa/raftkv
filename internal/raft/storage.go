package raft

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// MemoryStorage keeps everything in RAM. It is for tests: a test can "crash"
// a node and start a new one on the same MemoryStorage to simulate a restart.
type MemoryStorage struct {
	mu       sync.Mutex
	term     uint64
	votedFor string
	entries  []Entry
}

func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{} }

func (m *MemoryStorage) Load() (uint64, string, []Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.term, m.votedFor, append([]Entry(nil), m.entries...), nil
}

func (m *MemoryStorage) SaveState(term uint64, votedFor string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.term, m.votedFor = term, votedFor
	return nil
}

func (m *MemoryStorage) Append(entries []Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = append(m.entries, entries...)
	return nil
}

func (m *MemoryStorage) TruncateFrom(index uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if index >= 1 && index-1 < uint64(len(m.entries)) {
		m.entries = m.entries[:index-1]
	}
	return nil
}

// FileStorage persists Raft state in a directory:
//
//	state.json  current term and vote, replaced atomically on every change
//	log         append-only entries, each protected by a checksum
//
// Log record layout (little endian):
//
//	crc32 (4) | term (8) | index (8) | cmdLen (4) | cmd
type FileStorage struct {
	mu      sync.Mutex
	dir     string
	logFile *os.File
	offsets []int64 // offsets[i] is where entry with Index i+1 starts
	size    int64
}

const recordHeader = 24

var crcTable = crc32.MakeTable(crc32.Castagnoli)

func OpenFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "log"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	return &FileStorage{dir: dir, logFile: f}, nil
}

func (s *FileStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.logFile.Close()
}

type stateFile struct {
	Term     uint64 `json:"term"`
	VotedFor string `json:"voted_for"`
}

func (s *FileStorage) Load() (uint64, string, []Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var st stateFile
	data, err := os.ReadFile(filepath.Join(s.dir, "state.json"))
	if err == nil {
		if err := json.Unmarshal(data, &st); err != nil {
			return 0, "", nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return 0, "", nil, err
	}

	if _, err := s.logFile.Seek(0, io.SeekStart); err != nil {
		return 0, "", nil, err
	}
	var entries []Entry
	s.offsets, s.size = nil, 0
	r := bufio.NewReader(s.logFile)
	var hdr [recordHeader]byte
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			break // end of file, or a header cut short by a crash
		}
		cmdLen := binary.LittleEndian.Uint32(hdr[20:24])
		if cmdLen > 64<<20 {
			break
		}
		cmd := make([]byte, cmdLen)
		if _, err := io.ReadFull(r, cmd); err != nil {
			break
		}
		crc := crc32.Update(crc32.Update(0, crcTable, hdr[4:]), crcTable, cmd)
		if crc != binary.LittleEndian.Uint32(hdr[0:4]) {
			break
		}
		e := Entry{Term: binary.LittleEndian.Uint64(hdr[4:12]), Index: binary.LittleEndian.Uint64(hdr[12:20])}
		if cmdLen > 0 {
			e.Cmd = cmd
		}
		entries = append(entries, e)
		s.offsets = append(s.offsets, s.size)
		s.size += recordHeader + int64(cmdLen)
	}
	// Drop a torn record left by a crash mid-append.
	if err := s.logFile.Truncate(s.size); err != nil {
		return 0, "", nil, err
	}
	return st.Term, st.VotedFor, entries, nil
}

func (s *FileStorage) SaveState(term uint64, votedFor string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, _ := json.Marshal(stateFile{term, votedFor})
	tmp := filepath.Join(s.dir, "state.json.tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	// Rename is atomic: after a crash the file holds the old vote or the new
	// one, never half of each.
	return os.Rename(tmp, filepath.Join(s.dir, "state.json"))
}

func (s *FileStorage) Append(entries []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var buf []byte
	start := s.size
	for _, e := range entries {
		var hdr [recordHeader]byte
		binary.LittleEndian.PutUint64(hdr[4:12], e.Term)
		binary.LittleEndian.PutUint64(hdr[12:20], e.Index)
		binary.LittleEndian.PutUint32(hdr[20:24], uint32(len(e.Cmd)))
		crc := crc32.Update(crc32.Update(0, crcTable, hdr[4:]), crcTable, e.Cmd)
		binary.LittleEndian.PutUint32(hdr[0:4], crc)
		s.offsets = append(s.offsets, start+int64(len(buf)))
		buf = append(buf, hdr[:]...)
		buf = append(buf, e.Cmd...)
	}
	if _, err := s.logFile.WriteAt(buf, start); err != nil {
		return err
	}
	s.size += int64(len(buf))
	return s.logFile.Sync()
}

func (s *FileStorage) TruncateFrom(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index < 1 || index-1 >= uint64(len(s.offsets)) {
		return nil
	}
	s.size = s.offsets[index-1]
	s.offsets = s.offsets[:index-1]
	if err := s.logFile.Truncate(s.size); err != nil {
		return err
	}
	return s.logFile.Sync()
}
