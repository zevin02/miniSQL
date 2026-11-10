package transaction

import (
	bm "miniSQL/buffer_manager"
	fm "miniSQL/file_manager"
	lm "miniSQL/log_manager"
	"testing"

	"github.com/stretchr/testify/require"
)

// Use fixture-local transaction IDs so these tests do not depend on or advance
// the global transaction counter used by the existing transaction tests.
type recoveryFixture struct {
	t       *testing.T
	dir     string
	file    *fm.FileManager
	log     *lm.LogManager
	buffers *bm.BufferManager
	nextID  int32
	blk     *fm.BlockId
}

func newRecoveryFixture(t *testing.T) *recoveryFixture {
	f := &recoveryFixture{t: t, dir: t.TempDir(), nextID: 1000}
	f.reopen()
	blk, err := f.file.Append("recovery_data")
	require.NoError(t, err)
	f.blk = &blk
	tx := f.newTransaction()
	f.write(tx, 10, "initial", false)
	tx.Commit()
	return f
}

func (f *recoveryFixture) reopen() {
	f.t.Helper()
	var err error
	f.file, err = fm.NewFileManager(f.dir, 400)
	require.NoError(f.t, err)
	f.log, err = lm.NewLogManager(f.file, "recovery_log")
	require.NoError(f.t, err)
	f.buffers = bm.NewBufferManager(f.file, f.log, 3)
}

func (f *recoveryFixture) newTransaction() *Transaction {
	f.nextID++
	tx := &Transaction{
		fileManager:   f.file,
		logManager:    f.log,
		bufferManager: f.buffers,
		myBuffers:     NewBufferList(f.buffers),
		txNum:         f.nextID,
		concurrentMgr: NewConcurrencyManager(),
	}
	tx.recoverManager = NewRecoverManager(tx, tx.txNum, f.log, f.buffers)
	return tx
}

func (f *recoveryFixture) write(tx *Transaction, number int64, value string, logged bool) {
	f.t.Helper()
	if tx.myBuffers.getBuf(f.blk) == nil {
		require.NoError(f.t, tx.Pin(f.blk))
	}
	require.NoError(f.t, tx.SetInt(f.blk, 0, number, logged))
	require.NoError(f.t, tx.SetString(f.blk, 16, value, logged))
}

// Flush the loser's WAL and dirty page without a commit/rollback record, then
// discard all managers. Releasing its in-memory locks models process exit.
func (f *recoveryFixture) crash(tx *Transaction) {
	f.t.Helper()
	tx.myBuffers.UnpinAll()
	tx.concurrentMgr.Release()
	require.NoError(f.t, f.log.Flush())
	f.reopen()
}

func (f *recoveryFixture) recoverAndCheck(number int64, value string) {
	f.t.Helper()
	tx := f.newTransaction()
	defer func() {
		tx.myBuffers.UnpinAll()
		tx.concurrentMgr.Release()
	}()
	require.NoError(f.t, tx.Recover())
	require.NoError(f.t, tx.Pin(f.blk))
	actualNumber, err := tx.GetInt(f.blk, 0)
	require.NoError(f.t, err)
	require.Equal(f.t, number, actualNumber)
	actualValue, err := tx.GetString(f.blk, 16)
	require.NoError(f.t, err)
	require.Equal(f.t, value, actualValue)

	// Verify the recovered values were persisted, not just changed in memory.
	p := fm.NewPageBySize(f.file.BlockSize())
	_, err = f.file.Read(f.blk, p)
	require.NoError(f.t, err)
	require.Equal(f.t, number, p.GetInt(0))
	require.Equal(f.t, value, p.GetString(16))
}

func TestRecoveryPreservesCommittedUpdates(t *testing.T) {
	f := newRecoveryFixture(t)
	tx := f.newTransaction()
	f.write(tx, 20, "committed", true)
	tx.Commit()
	f.reopen()
	f.recoverAndCheck(20, "committed")
}

func TestRecoveryUndoesOnlyUnfinishedUpdates(t *testing.T) {
	f := newRecoveryFixture(t)
	committed := f.newTransaction()
	f.write(committed, 20, "committed", true)
	committed.Commit()

	unfinished := f.newTransaction()
	f.write(unfinished, 30, "unfinished first", true)
	f.write(unfinished, 40, "unfinished second", true)
	f.crash(unfinished)
	f.recoverAndCheck(20, "committed")
}

func TestRecoveryDoesNotReplayRolledBackUpdates(t *testing.T) {
	f := newRecoveryFixture(t)
	rolledBack := f.newTransaction()
	f.write(rolledBack, 20, "rollback first", true)
	f.write(rolledBack, 30, "rollback second", true)
	require.NoError(t, rolledBack.RollBack())

	committed := f.newTransaction()
	f.write(committed, 40, "later committed", true)
	committed.Commit()
	f.reopen()
	f.recoverAndCheck(40, "later committed")
}

func TestRecoveryStopsAtLatestCheckpoint(t *testing.T) {
	f := newRecoveryFixture(t)
	unfinished := f.newTransaction()
	f.write(unfinished, 20, "unfinished", true)
	f.crash(unfinished)
	f.recoverAndCheck(10, "initial")

	// The first recovery wrote a checkpoint after undoing the unfinished tx.
	// A subsequent recovery must not replay that tx's old undo records over
	// changes committed after the checkpoint.
	committed := f.newTransaction()
	f.write(committed, 30, "after checkpoint", true)
	committed.Commit()
	f.reopen()
	f.recoverAndCheck(30, "after checkpoint")
	f.reopen()
	f.recoverAndCheck(30, "after checkpoint")
}
