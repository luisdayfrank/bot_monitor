"""
tests/test_f12_semana4_labels_netos.py - F12.1 (Plan v7.4, Semana 4)

Regla general: todo label de rentabilidad usa retorno NETO de fees.
Verifica los labelers fuera de F12.2/2-bis/3:
  - ExitOutcomeLabeler: pnl_neto_pct = pnl_pct - 2*fee
  - OutcomeLabeler:     ret_4h_neto_pct = ret_4h_pct - 2*fee
  - FireOutcomeImporter: cubierto por tests/test_f06_labels.py (ret_neto_*)
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import learning_engine as le  # noqa: E402


def _test_exit_labeler():
    lab = le.ExitOutcomeLabeler(fee_pct=0.05)
    lab.registrar_entrada('p1', 'XUSDT', 'LONG', 100.0, qty=1.0)
    fila = lab.registrar_salida('p1', 101.0)
    assert fila is not None
    assert abs(fila['pnl_pct'] - 1.0) < 1e-6, fila['pnl_pct']
    assert abs(fila['fees_pct'] - 0.10) < 1e-6, fila['fees_pct']
    assert abs(fila['pnl_neto_pct'] - 0.90) < 1e-6, fila['pnl_neto_pct']
    print('  [OK] F12.1: ExitOutcomeLabeler pnl_neto_pct = 0.90')


def _test_outcome_labeler():
    lab = le.OutcomeLabeler(velas_loader=None, fee_pct=0.05)
    ts = 1_700_000_000_000
    snap = {'id': 1, 'symbol': 'XUSDT', 'timestamp_15m': ts,
            'price': 100.0, 'atr_pct_15m': 1.0}
    velas = [{'timestamp': ts + 900_000 * k, 'open': 100.0, 'high': 101.0,
              'low': 100.0, 'close': 101.0} for k in range(1, 33)]
    fila = lab._etiquetar(snap, velas)
    assert fila is not None
    assert abs(fila['ret_4h_pct'] - 1.0) < 1e-6, fila['ret_4h_pct']
    assert abs(fila['ret_4h_neto_pct'] - 0.90) < 1e-6, fila['ret_4h_neto_pct']
    print('  [OK] F12.1: OutcomeLabeler ret_4h_neto_pct = 0.90')


def main():
    _test_exit_labeler()
    _test_outcome_labeler()
    print('F12 semana 4 labels netos: PASS')


if __name__ == '__main__':
    main()
