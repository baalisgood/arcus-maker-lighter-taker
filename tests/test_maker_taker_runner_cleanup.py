"""Actual CLI failure cleanup and final status; local fixture frames, no network."""
import json
import sqlite3
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

import run_maker_taker
from core.services.arbitrage_monitor_v2.execution.maker_taker_engine import MakerTakerEngine
from core.services.arbitrage_monitor_v2.execution.maker_taker_journal import Journal
from core.services.arbitrage_monitor_v2.execution.maker_taker_paper import PublicMarkets

SYMBOL='SOL-USDC-PERP'
MARKER='synthetic-private-storage-marker'


@pytest.mark.parametrize('fault',['inventory','all_event_writes','stop','source_close','journal_close'])
def test_cli_final_status_and_cleanup_survive_each_storage_or_close_failure(tmp_path,monkeypatch,capsys,fault):
    data=json.loads(Path('tests/fixtures/maker_taker_replay.json').read_text());closed=[]
    original_close=Journal.close
    def journal_close(self):
        closed.append('journal');original_close(self)
        if fault=='journal_close':raise RuntimeError(MARKER)
    class LocalFrames(PublicMarkets):
        async def metadata(self):
            self.load_metadata(data['arcus_markets'],data['lighter_markets'])
            self.connected={'arcus':True,'lighter':True};self.generations={'arcus':1,'lighter':1};return self.rules
        async def start(self,callback):
            self.callback=callback
            for frame in data['initial_books']:self._publish(frame['venue'],frame['symbol'],frame['bids'],frame['asks'])
        async def close(self):
            closed.append('source');await super().close()
            if fault=='source_close':raise RuntimeError(MARKER)
    async def failed_tick(self):
        if fault=='inventory':
            def broken(*args):raise sqlite3.OperationalError(MARKER)
            self.journal.inventory=broken
        elif fault=='all_event_writes':
            self.journal.db.execute("CREATE TRIGGER storage_failure BEFORE INSERT ON events BEGIN SELECT RAISE(ABORT,'"+MARKER+"'); END")
        try:self.serious_halt('retry_exhausted',SYMBOL,None)
        finally:await self.cancel_all()
    if fault=='stop':
        async def failed_stop(self,*args,**kwargs):raise sqlite3.OperationalError(MARKER)
        monkeypatch.setattr(MakerTakerEngine,'stop',failed_stop)
    monkeypatch.setattr(MakerTakerEngine,'tick',failed_tick)
    monkeypatch.setattr('core.services.arbitrage_monitor_v2.execution.maker_taker_paper.PublicMarkets',LocalFrames)
    monkeypatch.setattr('core.services.arbitrage_monitor_v2.history.maker_taker_statistics.statistics_background',AsyncMock(return_value=[]))
    monkeypatch.setattr(Journal,'close',journal_close)
    assert run_maker_taker.main(['--mode','monitor','--duration-seconds','.1','--state-dir',str(tmp_path/'runner')]) in (1,2)
    captured=capsys.readouterr()
    rows=[json.loads(line) for line in captured.out.splitlines() if line.startswith('{')]
    final=rows[-1]
    assert final['shutdown_clean'] is False and final['manual_intervention_required'] is True
    snapshot=final['manual_intervention_snapshot']
    assert snapshot['failure_symbol']==SYMBOL and snapshot['reason_code']=='retry_exhausted'
    assert final['manual_intervention_persisted']==(fault!='all_event_writes')
    if fault=='inventory':assert snapshot['net_exposure_qty'] is None
    assert closed==['source','journal'] and MARKER not in captured.out+captured.err
    if fault in ('source_close','journal_close'):
        assert final['cleanup_errors']==[{'stage':fault.replace('_','.'),'error_type':'RuntimeError'}]
