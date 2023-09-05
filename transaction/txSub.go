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

func (t *TxStub) Unpin(_ *fm.BlockId) {

}
func (t *TxStub) GetInt(_ *fm.BlockId, offset uint64) (int64, error) {

	return t.p.GetInt(offset), nil
}
func (t *TxStub) GetString(_ *fm.BlockId, offset uint64) (string, error) {
	val := t.p.GetString(offset)
	return val, nil
}

func (t *TxStub) SetInt(_ *fm.BlockId, offset uint64, val int64, _ bool) error {
	t.p.SetInt(offset, val)
	return nil
}

func (t *TxStub) SetString(_ *fm.BlockId, offset uint64, val string, _ bool) error {
	t.p.SetString(offset, val)
	return nil
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
