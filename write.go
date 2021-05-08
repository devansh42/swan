package main

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	// Number of records will reside in buffer memory until the next flush
	BufferedRecordsLimit = 20

	// Number of records log file can sustain
	LogFileRecordLimit = BufferedRecordsLimit * 10

	// Number of records in lsm base
	LsmRecordLimitBase     = LogFileRecordLimit * 10
	KeyLength          int = 16
	IntLength          int = 4
	Int64Length        int = 8
	MBSize                 = 1024 * 1024
	CopyBufferSize         = 32 * 1024 // 32 Kb
	MaxLevel               = 7
	WriteBufferSize        = 4 * 1024 // 4 Kb
)

type (
	metricKey string

	// Write Format
	// Column oriented records sorted by, key_name > time_stamp

	// Header: [start_time][end_time][record_count][key_count]
	// [key1][key2][key3]...
	// [record_count(key1)][record_count(key2)][record_count(key3)]...
	// [ts_k1_r1][ts_k1_r2][ts_k1_rn][ts_k2_r1][ts_k2_r2][ts_k2_rn]....
	// [k1_r1_ln][k1_r2_ln][k1_rn_ln][k2_r1_ln][k2_r2_ln][k2_rn_ln]
	// [k1_r1][k1_r2][k1_rn][k2_r1][k2_r2][k2_rn]
	lsmFile struct {
		metaInfo lsmMetaInfo
		file     interface {
			io.ReadWriteCloser
			io.Seeker
		}
	}

	// Write Format
	// [start_time][end_time][number_of_records]
	lsmMetaInfo struct {
		start, end   int64
		totalRecords int
		keycount     int
	}
	// Write Format
	// [timestamp][key][length_of_value][value_bytes]
	record struct {
		ts    int64
		name  string
		value string
	}

	db struct {
		logFile                 *os.File
		logWriter               bufio.Writer
		lsmFiles, younglsmFiles []lsmFile

		loggedBufferedRecords, logFileRecords int
		manifest                              manifest
	}
	records          []record
	preSortedRecords [][]record
	manifest         struct {
		levels     int
		compacting bool
	}
)

func (m manifest) WriteTo(w io.Writer) {
	if m.compacting {
		m.levels &= 1
	}
	w.Write(intToBytes(m.levels))
}

func (m *manifest) ReadFrom(r io.Reader) {
	var b = make([]byte, IntLength)
	r.Read(b)
	m.levels = bytesToint(b)
	if m.levels&1 == 1 {
		m.compacting = true
	}
	m.levels = (m.levels >> 1) << 1
}

func (m manifest) levelExists(level int) bool {
	x := 1 << level
	return m.levels&x == x
}
func (m *manifest) setLevel(level int) {
	m.levels |= 1 << level
}
func (m *manifest) unsetLevel(level int) {
	x := 1 << level
	m.levels &= ^x

}

func (r records) Len() int {
	return len(r)
}

func (r records) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r records) Less(i, j int) bool {
	return r[i].ts < r[j].ts
}

func (p preSortedRecords) Len() int {
	return len(p)
}
func (r preSortedRecords) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}
func (p preSortedRecords) Less(i, j int) bool {
	return strings.Compare(p[i][0].name, p[j][0].name) == -1
}

func (m *lsmMetaInfo) ReadFrom(r io.Reader) {
	var b [24]byte
	_, err := r.Read(b[:])
	if err != nil {
		// Handling gracefully
	}
	m.start = bytesToint64(b[:8])
	m.end = bytesToint64(b[8:16])
	m.totalRecords = bytesToint(b[16:20])
	m.keycount = bytesToint(b[20:])
}

func (m lsmMetaInfo) WriteTo(w io.Writer) {

	w.Write(int64ToBytes(m.start))
	w.Write(int64ToBytes(m.end))
	w.Write(intToBytes(m.totalRecords))
	w.Write(intToBytes(m.keycount))
}

// This is a naive approach for lsm record count
func lsmRecordCount(level int) int {
	switch level {
	case 0:
		return LsmRecordLimitBase
	default:
		return LsmRecordLimitBase * level
	}
}

func (m metricKey) Hash() []byte {
	x := md5.Sum([]byte(m))
	return x[:]
}

var cacheRecord []record

func bytesToint64(b []byte) int64 {
	var c int64
	for i := 0; i < 8; i++ {
		c |= int64(b[i])
		c <<= 8
	}
	return c
}

func bytesToint(b []byte) int {
	var c int
	for i := 0; i < 4; i++ {
		c |= int(b[i])
		c <<= 8
	}
	return c

}

func int64ToBytes(x int64) []byte {
	var b [8]byte
	var c int64
	c = 1 << 8
	c -= 1
	for i := 0; i < 8; i++ {
		b[i] = byte(x & c)
		x >>= 8
	}
	return b[:]
}

func intToBytes(x int) []byte {
	var b [4]byte
	var c int
	c = 1 << 8
	c -= 1
	for i := 0; i < 4; i++ {
		b[i] = byte(x & c)
		x >>= 8
	}
	return b[:]
}

func (r record) WriteTo(w io.Writer) {
	mname := metricKey(r.name)
	bts := int64ToBytes(r.ts)
	lv := intToBytes(len(r.value))

	w.Write(bts)
	w.Write(mname.Hash())
	w.Write(lv)
	w.Write([]byte(r.value))
}

func (m *record) ReadFrom(r io.Reader) {
	var b [28]byte
	_, err := r.Read(b[:])
	if err != nil {
		// Handling gracefully
	}
	m.ts = bytesToint64(b[:8])
	m.name = string(b[8:24])
	lv := bytesToint(b[24:])
	var bb = make([]byte, lv)
	_, err = r.Read(bb)
	if err != nil {
		// Handling it gracefully
	}
}

// Writes in DB are in row formats, in lsm file it will column based
// Log Entry Format
// Because there are no updates, timeseries database are append only
func (db *db) write(name, value string) {
	r := record{ts: time.Now().Unix(), name: name, value: value}
	r.WriteTo(&db.logWriter)
	db.loggedBufferedRecords++
	if db.loggedBufferedRecords == BufferedRecordsLimit {
		err := db.logWriter.Flush()
		if err != nil {
			// Handle this properly
		}
		db.loggedBufferedRecords = 0 // Reseting the log records counter
		db.logFileRecords += BufferedRecordsLimit
		if db.logFileRecords == LogFileRecordLimit {
			// It's time to push the data to lsm file
			db.writeToLSM(0)
		}
	}
}

// writeToLSM will read the records from log file and will push them in lsm
// after sorting them
// for simplicity we will first make a level 0 lsm file w/o compactation
// capabilities
// level 0 is log file
//TODO  Seperate file pointer while reading and writting from lsm file
func (db *db) writeToLSM(level int) {

	// Lets read the records from log file
	breader := bufio.NewReader(db.logFile)

	var logRecords []record
	for breader.Buffered() != 0 {
		var r record
		r.ReadFrom(breader)
		logRecords = append(logRecords, r)
	}
	newFile := createNewFile()
	defer newFile.Close()
	buf := bufio.NewWriter(newFile)
	writeLogFileInLsmFormat(sortLogFileRecords(logRecords), len(logRecords), buf) // This is a Level 0 File
	defer buf.Flush()
}

func (db *db) compact() {

	for level := 0; level < MaxLevel && needCompactation(db.lsmFiles[level].file, level); level++ {
		levelExists := db.manifest.levelExists(level + 1)
		if !levelExists {
			db.lsmFiles[level+1].file = createNewFile()
		}
		newFile := createNewFile()
		wr := bufio.NewWriterSize(newFile, WriteBufferSize)
		mergeLSMFiles(db.lsmFiles[level].file, db.lsmFiles[level+1].file, wr)
		wr.Flush() // Flushing to FS
		db.lsmFiles[level+1].file = newFile
		db.manifest.setLevel(level + 1)
		db.manifest.unsetLevel(level)
	}

}
func mergeLSMFiles(a, b io.ReadSeeker, wr io.Writer) {
	var (
		aH, bH, newHeader, oldHeader lsmMetaInfo
		new, old                     io.ReadSeeker
	)
	a.Seek(0, 0)
	b.Seek(0, 0)
	aH.ReadFrom(a)
	bH.ReadFrom(b)
	if aH.end < bH.end {
		new = a
		old = b
		newHeader = aH
		oldHeader = bH
	} else {
		new = b
		old = a
		newHeader = bH
		oldHeader = aH

	}

	destHeader := lsmMetaInfo{start: oldHeader.start,
		end: newHeader.end, totalRecords: newHeader.totalRecords + oldHeader.totalRecords}
	var m = make(map[string][4]int)
	var oldkeyb = make([]byte, (KeyLength+IntLength)*oldHeader.keycount)
	old.Read(oldkeyb)
	olastKeyAddr := KeyLength * oldHeader.keycount
	for i := 0; i < oldHeader.keycount; i++ {
		start := i * KeyLength
		k := oldkeyb[start : start+KeyLength]
		x := olastKeyAddr + i*IntLength
		c := bytesToint(oldkeyb[x : x+IntLength])
		m[string(k)] = [4]int{-1, i, c, 0}
	}
	var newkeyb = make([]byte, (KeyLength+IntLength)*newHeader.keycount)
	new.Read(newkeyb)
	nlastKeyAddr := KeyLength * newHeader.keycount

	for i := 0; i < newHeader.keycount; i++ {
		start := i * KeyLength
		k := newkeyb[start : start+KeyLength]
		x := nlastKeyAddr + i*IntLength
		c := bytesToint(newkeyb[x : x+IntLength])

		v, ok := m[string(k)]
		if ok {
			v[0] = i
			v[3] = c
		} else {
			v = [4]int{i, -1, 0, c}
		}
		m[string(k)] = v
	}
	var ks = make(keys, 0, len(m))
	i := 0
	for k := range m {
		ks[i] = key{key: k, new: m[k][0], old: m[k][1], orecords: m[k][2], nrecords: m[k][3]}
		i++
	}
	sort.Sort(ks)
	destHeader.keycount = len(m)
	destHeader.WriteTo(wr) // Writting header

	// Let's merge these files

	for v := range ks { // Writting keys
		wr.Write([]byte(ks[v].key))
	}
	for v := range ks { // Writting Values Count
		wr.Write(intToBytes(ks[v].orecords + ks[v].nrecords))
	}
	copyBuffer := make([]byte, CopyBufferSize)

	oseekIndex := (2*(IntLength+Int64Length) + (KeyLength+IntLength)*oldHeader.keycount)
	nseekIndex := (2*(IntLength+Int64Length) + (KeyLength+IntLength)*newHeader.keycount)

	for _, v := range ks { // Writting timestamp values
		o := v.old
		n := v.new
		if o > -1 {
			old.Seek(io.SeekStart, oseekIndex+o*Int64Length)
			io.CopyBuffer(wr, io.LimitReader(old, int64(v.orecords)*int64(Int64Length)), copyBuffer)
		}
		if n > -1 {
			new.Seek(io.SeekStart, nseekIndex+n*Int64Length)
			io.CopyBuffer(wr, io.LimitReader(new, int64(v.nrecords)*int64(Int64Length)), copyBuffer)

		}
	}
	oseekIndex += oldHeader.totalRecords * Int64Length
	nseekIndex += newHeader.totalRecords * Int64Length
	maxLength := 0
	for i := range ks {
		sum := ks[i].nrecords + ks[i].orecords
		if maxLength < sum {
			maxLength = sum
		}
	}
	bf := bytes.NewBuffer(make([]byte, maxLength*IntLength))

	for _, v := range ks { // Writting values lengths
		o := v.old
		n := v.new
		if o > -1 {
			old.Seek(io.SeekStart, oseekIndex+o*IntLength)
			io.CopyBuffer(io.MultiWriter(bf, wr), io.LimitReader(old, int64(v.orecords)*int64(IntLength)), copyBuffer)

		}
		if n > -1 {
			new.Seek(io.SeekStart, nseekIndex+n*IntLength)
			io.CopyBuffer(io.MultiWriter(bf, wr), io.LimitReader(new, int64(v.nrecords)*int64(IntLength)), copyBuffer)
		}
		buf := make([]byte, IntLength)

		for i := 0; i < v.orecords; i++ {
			b.Read(buf)
			v.osum += bytesToint(buf)
		}
		for i := 0; i < v.nrecords; i++ {
			b.Read(buf)
			v.nsum += bytesToint(buf)
		}

	}

	oseekIndex += oldHeader.totalRecords * IntLength
	nseekIndex += newHeader.totalRecords * IntLength

	for _, v := range ks { // Writting  values
		o := v.old
		n := v.new
		if o > -1 {
			old.Seek(io.SeekStart, oseekIndex+o*Int64Length)
			io.CopyBuffer(wr, io.LimitReader(old, int64(v.osum)), copyBuffer)
		}
		if n > -1 {
			new.Seek(io.SeekStart, nseekIndex+n*Int64Length)
			io.CopyBuffer(wr, io.LimitReader(new, int64(v.nsum)), copyBuffer)

		}
	}

}

type key struct {
	key                                      string
	old, new, orecords, nrecords, osum, nsum int
}
type keys []key

func (k keys) Less(i, j int) bool {
	return strings.Compare(k[i].key, k[j].key) == -1
}
func (k keys) Swap(i, j int) {
	k[i], k[j] = k[j], k[i]
}
func (k keys) Len() int {
	return len(k)
}

// Writes log file data into lsm file format
func writeLogFileInLsmFormat(recrodsToaadd [][]record, length int, wr io.Writer) {
	first := recrodsToaadd[0][0]
	l := recrodsToaadd[len(recrodsToaadd)-1]
	last := recrodsToaadd[len(recrodsToaadd)-1][len(l)-1]
	var header = lsmMetaInfo{start: first.ts,
		end:          last.ts,
		keycount:     len(recrodsToaadd),
		totalRecords: length}
	header.WriteTo(wr) // Writting

	for i := range recrodsToaadd { // Writting Keys
		wr.Write([]byte(recrodsToaadd[i][0].name))
	}
	for i := range recrodsToaadd { // Writting Value Length
		l := len(recrodsToaadd[i])
		wr.Write(intToBytes(l))
	}
	for i := range recrodsToaadd { // Writting Timestamp
		for ii := range recrodsToaadd[i] {
			wr.Write(int64ToBytes(recrodsToaadd[i][ii].ts))
		}
	}
	for i := range recrodsToaadd { // Writting Length Values
		for ii := range recrodsToaadd[i] {
			wr.Write(intToBytes(len(recrodsToaadd[i][ii].value)))
		}
	}
	for i := range recrodsToaadd { // Writting Values
		for ii := range recrodsToaadd[i] {
			wr.Write([]byte(recrodsToaadd[i][ii].value))
		}
	}

}

func sortLogFileRecords(rds []record) [][]record {
	var m = make(map[string][]record)
	for i := range rds {
		m[rds[i].name] = append(m[rds[i].name], rds[i])
	}
	var mm = make([][]record, len(m))
	i := 0
	for k := range m {
		rs := records(m[k])
		sort.Sort(rs)
		mm[i] = rs
		i++
	}
	mmp := preSortedRecords(mm)
	sort.Sort(mmp)
	return [][]record(mm)
}

func needCompactation(seeker io.Seeker, level int) bool {
	beg, _ := seeker.Seek(io.SeekStart, 0)
	end, _ := seeker.Seek(io.SeekEnd, 0)
	size := end - beg + 1
	return size > int64(math.Pow10(level))*MBSize
}

func createNewFile() *os.File {
	name := strconv.FormatInt(time.Now().Unix(), 6)
	//TODO Handle Error
	b := md5.Sum([]byte(name))
	f, _ := os.Create(string(b[:]))
	return f
}
