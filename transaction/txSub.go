package transaction

import fm "miniSQL/file_manager"

type TxStub struct {
	p *fm.Page
}

func (t *TxStub) AvailableBuffer() uint64 {
	//TODO implement me
	return 0
}

func NewTxStub(p *fm.Page) *TxStub {
	return &TxStub{
		p: p,
	}
}

func (t *TxStub) Commit() {

}

func (t *TxStub) RollBack() {

}

func (t *TxStub) Recover() {

}

func (t *TxStub) Pin(_ *fm.BlockId) {

}

func (t *TxStub) UnPin(_ *fm.BlockId) {

}
func (t *TxStub) GetInt(_ *fm.BlockId, offset uint64) uint64 {

	return t.p.GetInt(offset)
}
func (t *TxStub) GetString(_ *fm.BlockId, offset uint64) string {
	val := t.p.GetString(offset)
	return val
}

func (t *TxStub) SetInt(_ *fm.BlockId, offset uint64, val uint64, _ bool) {
	t.p.SetInt(offset, val)
}

func (t *TxStub) SetString(_ *fm.BlockId, offset uint64, val string, _ bool) {
	t.p.SetString(offset, val)
}

func (t *TxStub) Size(_ string) uint64 {
	return 0
}

func (t *TxStub) Append(_ string) *fm.BlockId {
	return nil
}

func (t *TxStub) BlockSize() uint64 {
	return 0
}
