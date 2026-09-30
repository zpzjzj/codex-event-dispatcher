import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import events
from controller import database, reserve_dispatch, finish_dispatch, config


class ExternalEventsTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = database(str(Path(self.temp.name) / 'db'))
        self.addCleanup(self.db.close)
        self.cfg = {'repositories': {}, 'sessions_root': self.temp.name, 'subscriptions': {
            'chat': {'enabled': True, 'type': 'command', 'command': ['collector'],
                     'routes': [{'resource': 'conversation-1', 'thread_id': 'thread-1'}]}}}
        self.event = {'id': 'message-1', 'resource': 'conversation-1', 'type': 'message.requested',
                      'context': {'text': 'Please investigate'}}

    def test_idempotency_lease_release_and_ack(self):
        self.assertEqual(events.ingest(self.db, self.cfg, 'chat', self.event), 1)
        self.assertEqual(events.ingest(self.db, self.cfg, 'chat', self.event), 0)
        batches = reserve_dispatch(self.db, self.cfg)
        self.assertEqual(len(batches), 1)
        batch = batches[0]
        self.assertEqual(batch['thread_id'], 'thread-1')
        self.assertIn('【Assistant Agent】', batch['prompt'])
        self.assertIn('Subscription activation is not approval', batch['prompt'])
        self.assertEqual(reserve_dispatch(self.db, self.cfg), [])
        with self.assertRaises(ValueError):
            finish_dispatch(self.db, batch['job_ids'], 'wrong-thread', True)
        self.assertEqual(finish_dispatch(self.db, batch['job_ids'], 'thread-1', False), 1)
        with self.db:
            self.db.execute('UPDATE jobs SET retry_after=0')
        next_batch = reserve_dispatch(self.db, self.cfg)[0]
        self.assertEqual(next_batch['job_ids'], batch['job_ids'])
        self.assertEqual(finish_dispatch(self.db, batch['job_ids'], 'thread-1', True), 1)
        self.assertEqual(self.db.execute('SELECT state FROM jobs').fetchone()[0], 'delegated')

    def test_unmatched_and_ambiguous_routes_are_retained(self):
        self.cfg['subscriptions']['chat']['routes'].append({'resource': 'conversation-1', 'thread_id': 'thread-2'})
        events.ingest(self.db, self.cfg, 'chat', self.event)
        self.assertEqual(reserve_dispatch(self.db, self.cfg), [])
        self.assertEqual(self.db.execute('SELECT state FROM jobs').fetchone()[0], 'waiting_route')
        events.bind(self.db, 'chat', 'conversation-1', 'thread-2', '')
        self.assertEqual(reserve_dispatch(self.db, self.cfg)[0]['thread_id'], 'thread-2')

    def test_disabled_unknown_and_malformed_sources_rejected(self):
        with self.assertRaises(ValueError):
            events.ingest(self.db, self.cfg, 'unknown', self.event)
        self.cfg['subscriptions']['chat']['enabled'] = False
        with self.assertRaises(ValueError):
            events.ingest(self.db, self.cfg, 'chat', self.event)
        self.cfg['subscriptions']['chat']['enabled'] = True
        with self.assertRaises(ValueError):
            events.ingest(self.db, self.cfg, 'chat', {**self.event, 'id': ''})
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM jobs').fetchone()[0], 0)

    def test_disabled_source_does_not_dispatch_old_jobs(self):
        events.ingest(self.db, self.cfg, 'chat', self.event)
        self.cfg['subscriptions']['chat']['enabled'] = False
        self.assertEqual(reserve_dispatch(self.db, self.cfg), [])

    def test_source_failure_does_not_block_other_collectors(self):
        self.cfg['subscriptions']['broken'] = {'type': 'command', 'enabled': True, 'command': ['fail']}
        with patch('events.run_json', side_effect=[[self.event], ValueError('private content')]):
            result = events.poll(self.db, self.cfg)
        self.assertEqual(result, {'new_jobs': 1, 'errors': {'broken': 'ValueError'}})

    def test_optional_baseline_then_new_event(self):
        self.cfg['subscriptions']['chat']['initial_baseline'] = True
        second = {**self.event, 'id': 'message-2'}
        with patch('events.run_json', return_value=[self.event]):
            self.assertEqual(events.poll(self.db, self.cfg, force=True)['new_jobs'], 0)
        with patch('events.run_json', return_value=[self.event, second]):
            self.assertEqual(events.poll(self.db, self.cfg, force=True)['new_jobs'], 1)
            self.assertEqual(events.poll(self.db, self.cfg, force=True)['new_jobs'], 0)

    def test_schedule_backoff_and_filters_are_model_free(self):
        source = self.cfg['subscriptions']['chat']
        source['event_types'] = ['review.updated']
        with patch('events.run_json', return_value=[self.event]) as runner:
            self.assertEqual(events.poll(self.db, self.cfg)['new_jobs'], 0)
            events.poll(self.db, self.cfg)
            self.assertEqual(runner.call_count, 1)
        with patch('events.run_json', side_effect=ValueError('private error')):
            self.assertEqual(events.poll(self.db, self.cfg, force=True)['errors'], {'chat': 'ValueError'})
        schedule = self.db.execute('SELECT * FROM source_schedule').fetchone()
        self.assertEqual(schedule['failures'], 1)

    def test_dispatch_cap_and_payload_preview(self):
        self.cfg['subscriptions']['chat']['max_events_per_dispatch'] = 2
        self.cfg['subscriptions']['chat']['context_preview_chars'] = 100
        for index in range(3):
            events.ingest(self.db, self.cfg, 'chat', {**self.event, 'id': str(index), 'context': {'text': 'x' * 4000}})
        batch = reserve_dispatch(self.db, self.cfg)[0]
        self.assertEqual(len(batch['job_ids']), 2)
        self.assertLess(len(batch['prompt']), 2200)
        self.assertIn('event-detail', batch['prompt'])

    def test_output_size_and_timeout_are_bounded(self):
        import sys
        import subprocess
        with self.assertRaises(ValueError):
            events.run_json([sys.executable, '-c', 'print("x" * 10000)'], max_output_bytes=256)
        with self.assertRaises(subprocess.TimeoutExpired):
            events.run_json([sys.executable, '-c', 'import time; time.sleep(5)'], timeout=0.05)
        self.assertEqual(events.run_json([sys.executable, '-c', 'print("[]")']), [])

    def test_invalid_limit_and_internal_adapter_type_rejected(self):
        self.cfg['subscriptions']['chat']['timeout_seconds'] = 0
        with self.assertRaises(ValueError):
            events.subscriptions(self.cfg)
        self.cfg['subscriptions']['chat']['timeout_seconds'] = 30
        self.cfg['subscriptions']['chat']['type'] = 'unsupported-adapter'
        with self.assertRaises(ValueError):
            events.subscriptions(self.cfg)

    def test_cli_collector_readiness_and_on_demand_detail(self):
        import subprocess
        import sys
        script = Path(__file__).with_name('controller.py')
        settings = {**self.cfg, 'database': str(Path(self.temp.name) / 'db')}
        settings['subscriptions']['chat']['command'] = [sys.executable, '-c',
            'print(' + repr(json.dumps([self.event])) + ')']
        path = Path(self.temp.name) / 'config.json'
        path.write_text(json.dumps(settings))
        def call(*args):
            return json.loads(subprocess.check_output([sys.executable, str(script), '--config', str(path), *args]))
        self.assertEqual(call('dispatch-ready')['ready_jobs'], 0)
        self.assertEqual(call('poll-sources')['new_jobs'], 1)
        self.assertEqual(call('poll-sources')['new_jobs'], 0)
        self.assertEqual(call('dispatch-ready')['ready_jobs'], 1)
        batch = call('reserve-dispatch')['batches'][0]
        self.assertEqual(call('event-detail', str(batch['job_ids'][0])), self.event)
        self.assertEqual(call('dispatch-ready')['ready_jobs'], 0)

    def test_config_accepts_external_only(self):
        path = Path(self.temp.name) / 'cfg.json'
        path.write_text(json.dumps(self.cfg))
        self.assertEqual(config(str(path))['repositories'], {})


if __name__ == '__main__':
    unittest.main()
